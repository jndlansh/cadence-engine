import { readFile } from "node:fs/promises";
import { parseWorkflowYAML } from "@cadence/core";
import {
  RedisCompletionSource,
  RedisWorkflowStateStore,
  RetryPromoter,
  TaskDispatcher,
  WorkflowEventLogger,
  WorkflowOrchestrator,
  type WorkflowRunStatus,
} from "@cadence/orchestrator";
import {
  ProgressStateStore,
  bridgeConsole,
  createProgressReporter,
} from "../progress.js";
import { ENV_ERROR, ensureRedis } from "../redis-check.js";
import { spawnWorkers, type WorkerPool } from "../workers.js";

export interface RunOptions {
  file: string;
  workers: number;
  workflowId?: string;
  timeoutMs: number;
  logFile?: string;
  progress: boolean;
  redisUrl: string;
}

/** Usage / parse error. */
export const USAGE_ERROR = 2;

const EXIT_CODE: Record<WorkflowRunStatus, number> = {
  COMPLETED: 0,
  FAILED: 1,
  STALLED: 3,
};

export function exitCodeFor(status: WorkflowRunStatus): number {
  return EXIT_CODE[status];
}

/**
 * `cadence run <file>`: parse the workflow, bring up N local workers, walk the
 * DAG, and render progress from the persisted lifecycle state. The orchestrator
 * runs in this process; progress comes from the `ProgressStateStore` decorator,
 * so no engine code is involved.
 */
export async function runCommand(options: RunOptions): Promise<number> {
  let workflow;
  try {
    workflow = parseWorkflowYAML(await readFile(options.file, "utf-8"));
  } catch (err) {
    console.error(
      `[cadence] ${err instanceof Error ? err.message : String(err)}`
    );
    return USAGE_ERROR;
  }

  // Confirm Redis before creating any client, worker, or stream, so an
  // unreachable server produces one clear message instead of a retry storm.
  if (!(await ensureRedis(options.redisUrl))) {
    return ENV_ERROR;
  }

  const workflowId =
    options.workflowId ?? `${workflow.definition.workflow}_${Date.now()}`;

  const reporter = createProgressReporter(options.progress);
  const restoreConsole = reporter.animated ? bridgeConsole(reporter) : (): void => {};

  const dispatcher = new TaskDispatcher(options.redisUrl);
  const completions = new RedisCompletionSource(options.redisUrl);
  const store = new ProgressStateStore(
    new RedisWorkflowStateStore(options.redisUrl),
    reporter
  );
  const promoter = new RetryPromoter(options.redisUrl, { pollIntervalMs: 300 });
  const logger = options.logFile
    ? new WorkflowEventLogger({ logFile: options.logFile, toConsole: false })
    : undefined;

  let pool: WorkerPool | null = null;

  const onSigint = (): void => {
    restoreConsole();
    const stopping = pool ? pool.stop() : Promise.resolve();
    void stopping.finally(() => process.exit(130));
  };
  process.once("SIGINT", onSigint);

  try {
    await dispatcher.ensureGroup();

    pool = await spawnWorkers({
      count: options.workers,
      redisUrl: options.redisUrl,
    });

    await promoter.start();

    console.log(
      `[cadence] workflow=${workflow.definition.workflow} id=${workflowId} ` +
        `tasks=${workflow.definition.tasks.length} workers=${options.workers}`
    );

    const orchestrator = new WorkflowOrchestrator(dispatcher, completions, {
      completionTimeoutMs: options.timeoutMs,
      stateStore: store,
      logger,
    });

    const result = await orchestrator.run(workflowId, workflow);

    const failedSuffix =
      result.failed.length > 0 ? `, failed [${result.failed.join(", ")}]` : "";
    console.log("");
    console.log(
      `[cadence] ${result.status}: ${result.completed.length}/` +
        `${workflow.definition.tasks.length} completed${failedSuffix}`
    );
    console.log(`[cadence] state key: workflow:${workflowId}:state`);
    console.log(`[cadence] inspect : pnpm cadence status ${workflowId}`);

    return exitCodeFor(result.status);
  } finally {
    process.off("SIGINT", onSigint);
    if (pool) {
      await pool.stop();
    }
    await promoter.disconnect();
    await completions.disconnect();
    await store.disconnect();
    // Every Redis connection must be closed or it keeps the event loop alive.
    await dispatcher.disconnect();
    restoreConsole();
    reporter.stop();
  }
}
