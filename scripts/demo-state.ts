/**
 * Phase 5 live demo: workflow lifecycle state is persisted, not just printed.
 *
 * Two runs against the same live stack:
 *   1. The demo diamond (extract -> transform_a/b -> load) finishes COMPLETED;
 *      the snapshot is read back from Redis and checked.
 *   2. A deliberately failing pipeline (boom -> never) ends FAILED; `never` is
 *      never dispatched, and the persisted `failed` list names `boom`.
 *
 * Run with:  pnpm exec tsx scripts/demo-state.ts
 *
 * Clear the streams and demo state keys first, exactly like the other demos —
 * the completion reader starts its cursor at 0, so a previous run's completions
 * would otherwise be replayed:
 *   memurai-cli DEL cadence:tasks cadence:tasks:dlq cadence:completions cadence:retries
 *   memurai-cli DEL workflow:demo_wf_1:state workflow:failed_wf_1:state
 */
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { parseWorkflowYAML, workflowStateKey } from "../packages/core/src/index.js";
import { TaskDispatcher } from "../packages/orchestrator/src/scheduler.js";
import { WorkflowOrchestrator } from "../packages/orchestrator/src/orchestrator.js";
import { RedisCompletionSource } from "../packages/orchestrator/src/completions.js";
import { RetryPromoter } from "../packages/orchestrator/src/promoter.js";
import { RedisWorkflowStateStore } from "../packages/orchestrator/src/state-store.js";
import { WorkflowEventLogger } from "../packages/orchestrator/src/event-logger.js";
import { WorkerConsumer } from "../packages/worker/src/consumer.js";

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";
const SUCCESS_WF = "demo_wf_1";
const FAILURE_WF = "failed_wf_1";
const COMPLETION_TIMEOUT_MS = 20_000;

const REPO_ROOT = process.cwd();
const WORKFLOW_FILE =
  process.argv[2] ?? join(REPO_ROOT, "workflows", "demo-pipeline.yaml");
const LOG_FILE = join(REPO_ROOT, "cadence-demo-state.log");

const FAILING_WORKFLOW = `
version: "1.0"
workflow: failing_pipeline
tasks:
  - id: boom
    command: 'node -e "process.exit(1)"'
    retries: 0
  - id: never
    command: echo never-should-run
    depends_on: [boom]
    retries: 0
`;

function fail(message: string): never {
  console.error(`\n[demo-state] FAILED: ${message}`);
  process.exit(1);
}

async function main(): Promise<void> {
  const successWorkflow = parseWorkflowYAML(
    await readFile(WORKFLOW_FILE, "utf-8")
  );
  const failureWorkflow = parseWorkflowYAML(FAILING_WORKFLOW);

  const dispatcher = new TaskDispatcher(REDIS_URL);
  await dispatcher.ensureGroup();

  const completions = new RedisCompletionSource(REDIS_URL);
  const store = new RedisWorkflowStateStore(REDIS_URL);
  const logger = new WorkflowEventLogger({ logFile: LOG_FILE });
  const promoter = new RetryPromoter(REDIS_URL, { pollIntervalMs: 300 });
  const worker = new WorkerConsumer(REDIS_URL, {
    backoff: { baseDelayMs: 500, maxDelayMs: 2000 },
  });

  const workerLoop = worker.start();
  await promoter.start();

  const orchestrator = new WorkflowOrchestrator(dispatcher, completions, {
    completionTimeoutMs: COMPLETION_TIMEOUT_MS,
    stateStore: store,
    logger,
  });

  try {
    // ---- Run 1: the happy path -------------------------------------------
    console.log(
      `\n[demo-state] run 1: ${successWorkflow.definition.workflow} ` +
        `(${successWorkflow.definition.tasks.length} tasks)`
    );
    const ok = await orchestrator.run(SUCCESS_WF, successWorkflow);

    if (ok.status !== "COMPLETED") {
      fail(`run 1 expected COMPLETED, got ${ok.status}`);
    }

    const persistedOk = await store.load(SUCCESS_WF);
    if (!persistedOk) {
      fail("run 1 persisted no state for " + SUCCESS_WF);
    }
    if (persistedOk.state !== "COMPLETED") {
      fail(`run 1 persisted state is ${persistedOk.state}, expected COMPLETED`);
    }
    for (const task of successWorkflow.definition.tasks) {
      const record = persistedOk.tasks[task.id];
      if (!record || record.status !== "COMPLETED") {
        fail(`run 1 persisted task ${task.id} as ${record?.status ?? "missing"}`);
      }
      if (record.attempt !== 1) {
        fail(`run 1 persisted task ${task.id} attempt ${record.attempt}`);
      }
      if (typeof record.latencyMs !== "number") {
        fail(`run 1 persisted no latency for task ${task.id}`);
      }
    }
    console.log(
      `[demo-state] run 1 persisted: state=${persistedOk.state} ` +
        `tasks=[${Object.keys(persistedOk.tasks).sort().join(", ")}]`
    );

    // ---- Run 2: terminal failure + fail-fast ------------------------------
    console.log(
      `\n[demo-state] run 2: ${failureWorkflow.definition.workflow} (expected FAILED)`
    );
    const failedRun = await orchestrator.run(FAILURE_WF, failureWorkflow);

    if (failedRun.status !== "FAILED") {
      fail(`run 2 expected FAILED, got ${failedRun.status}`);
    }
    const dispatchedNever = failedRun.events
      .filter((event) => event.type === "DISPATCHED")
      .flatMap((event) => event.taskIds)
      .includes("never");
    if (dispatchedNever) {
      fail("run 2 dispatched `never` despite its parent failing (fail-fast broken)");
    }

    const persistedFailed = await store.load(FAILURE_WF);
    if (!persistedFailed) {
      fail("run 2 persisted no state for " + FAILURE_WF);
    }
    if (persistedFailed.state !== "FAILED") {
      fail(`run 2 persisted state is ${persistedFailed.state}, expected FAILED`);
    }
    if (!persistedFailed.failed.includes("boom")) {
      fail("run 2 persisted failed list does not contain `boom`");
    }
    if (persistedFailed.tasks["never"]?.status !== "PENDING") {
      fail(
        `run 2 task \`never\` is ${persistedFailed.tasks["never"]?.status}, ` +
          `expected PENDING (never dispatched)`
      );
    }
    console.log(
      `[demo-state] run 2 persisted: state=${persistedFailed.state} ` +
        `failed=[${persistedFailed.failed.join(", ")}] ` +
        `never=${persistedFailed.tasks["never"].status}`
    );

    console.log("\n[demo-state] PASS");
    console.log(`[demo-state] structured log: ${LOG_FILE}`);
    console.log(
      `[demo-state] inspect: HGETALL ${workflowStateKey(SUCCESS_WF)} ` +
        `| HGET ${workflowStateKey(SUCCESS_WF)} tasks`
    );
  } finally {
    worker.requestShutdown();
    await workerLoop;
    await promoter.disconnect();
    await worker.disconnect();
    await logger.close();
    await completions.disconnect();
    await store.disconnect();
    await dispatcher.disconnect();
  }
}

main()
  .then(() => process.exit(0))
  .catch((err: unknown) => {
    console.error("[demo-state] fatal:", err);
    process.exit(1);
  });
