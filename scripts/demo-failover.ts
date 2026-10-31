/**
 * Phase 4 live demo: it proves GAP A is closed by actually killing a worker.
 *
 * Story:
 *   1. A real worker child process starts and heartbeats.
 *   2. The workflow dispatches `long_task`; the doomed worker claims it and is
 *      mid-execution when everything goes wrong.
 *   3. We SIGKILL the worker. It publishes no completion, so simply waiting
 *      would deadlock the orchestrator (the Phase 3.5 failure mode).
 *   4. The reclaimer notices the dead heartbeat, XCLAIMs the stranded entry,
 *      re-publishes it as attempt 2, and a healthy worker finishes it.
 *   5. The orchestrator receives the completion and advances to `after_task`.
 *
 * Run with:  pnpm exec tsx scripts/demo-failover.ts
 *
 * Clear the streams first if you want clean counts:
 *   memurai-cli DEL cadence:tasks cadence:tasks:dlq cadence:completions
 */
import { spawn, type ChildProcess } from "node:child_process";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { parseWorkflowYAML } from "../packages/core/src/parser.js";
import { TaskDispatcher } from "../packages/orchestrator/src/scheduler.js";
import { WorkflowOrchestrator } from "../packages/orchestrator/src/orchestrator.js";
import { RedisCompletionSource } from "../packages/orchestrator/src/completions.js";
import { TaskReclaimer } from "../packages/orchestrator/src/reclaimer.js";
import { RetryPromoter } from "../packages/orchestrator/src/promoter.js";
import { WorkerConsumer } from "../packages/worker/src/consumer.js";

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";
const WORKFLOW_ID = "failover_wf_1";

// Shrink the liveness windows so the demo fails-over in seconds, not minutes.
// The heartbeat TTL is the wait before the reclaimer may act on a dead worker.
const HEARTBEAT_TTL_SECONDS = 4;
const HEARTBEAT_INTERVAL_MS = 1000;
const MIN_IDLE_MS = 4000;
const COMPLETION_TIMEOUT_MS = 30_000;
// Backoff windows are irrelevant to this demo (nothing fails after reclaim),
// but a small base keeps any unexpected retry inside the completion timeout.
const RETRY_BASE_DELAY_MS = 500;
const RETRY_MAX_DELAY_MS = 2000;

const REPO_ROOT = process.cwd();
const WORKFLOW_FILE =
  process.argv[2] ?? join(REPO_ROOT, "workflows", "failover-pipeline.yaml");
const WORKER_SCRIPT = join(REPO_ROOT, "scripts", "worker-node.ts");

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitUntil(
  check: () => boolean | Promise<boolean>,
  timeoutMs: number,
  label: string
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await check()) return;
    if (Date.now() > deadline) {
      throw new Error(`timed out after ${timeoutMs}ms waiting for ${label}`);
    }
    await sleep(200);
  }
}

function waitForWorkerId(child: ChildProcess, timeoutMs: number): Promise<string> {
  return new Promise((resolve, reject) => {
    let buffer = "";
    const timer = setTimeout(() => {
      reject(new Error("worker child never announced its id"));
    }, timeoutMs);

    child.stdout?.on("data", (chunk: Buffer) => {
      buffer += chunk.toString("utf-8");
      const match = buffer.match(/WORKER_READY id=(\S+)/);
      if (match) {
        clearTimeout(timer);
        resolve(match[1]);
      }
    });
    child.on("exit", (code, signal) => {
      clearTimeout(timer);
      reject(
        new Error(`worker child exited early (code=${code} signal=${signal})`)
      );
    });
  });
}

function waitForExit(child: ChildProcess): Promise<void> {
  return new Promise((resolve) => {
    if (child.exitCode !== null || child.signalCode !== null) return resolve();
    child.once("exit", () => resolve());
  });
}

async function main(): Promise<void> {
  const raw = await readFile(WORKFLOW_FILE, "utf-8");
  const workflow = parseWorkflowYAML(raw);

  console.log(
    `[demo] workflow=${workflow.definition.workflow} ` +
      `tasks=[${workflow.definition.tasks.map((t) => t.id).join(", ")}]`
  );

  const dispatcher = new TaskDispatcher(REDIS_URL);
  await dispatcher.ensureGroup();

  const completions = new RedisCompletionSource(REDIS_URL);
  const reclaimer = new TaskReclaimer(REDIS_URL, {
    pollIntervalMs: 1000,
    minIdleMs: MIN_IDLE_MS,
  });
  const promoter = new RetryPromoter(REDIS_URL, { pollIntervalMs: 500 });
  const orchestrator = new WorkflowOrchestrator(dispatcher, completions, {
    completionTimeoutMs: COMPLETION_TIMEOUT_MS,
  });

  const child = spawn(
    process.execPath,
    ["--import", "tsx", WORKER_SCRIPT],
    {
      cwd: REPO_ROOT,
      env: {
        ...process.env,
        REDIS_URL,
        HEARTBEAT_TTL_SECONDS: String(HEARTBEAT_TTL_SECONDS),
        HEARTBEAT_INTERVAL_MS: String(HEARTBEAT_INTERVAL_MS),
        RETRY_BASE_DELAY_MS: String(RETRY_BASE_DELAY_MS),
        RETRY_MAX_DELAY_MS: String(RETRY_MAX_DELAY_MS),
      },
      stdio: ["ignore", "pipe", "pipe"],
    }
  );
  child.stdout?.on("data", (b: Buffer) => process.stdout.write(`[doomed] ${b}`));
  child.stderr?.on("data", (b: Buffer) => process.stderr.write(`[doomed] ${b}`));

  let workerB: WorkerConsumer | null = null;

  try {
    const workerId = await waitForWorkerId(child, 15_000);
    console.log(`[demo] doomed worker started: ${workerId}`);

    await waitUntil(
      () => reclaimer.isWorkerAlive(workerId),
      10_000,
      `${workerId} to register its heartbeat`
    );
    console.log(`[demo] ${workerId} heartbeat is live`);

    // Kick off the workflow: dispatches wave 1 (`long_task`).
    const runPromise = orchestrator.run(WORKFLOW_ID, workflow);

    await waitUntil(
      async () => {
        const pending = await reclaimer.listPending();
        return pending.some(([, consumer]) => consumer === workerId);
      },
      10_000,
      `${workerId} to claim long_task`
    );
    console.log(`[demo] ${workerId} claimed long_task and is running it`);

    // The whole point: a hard crash mid-task. SIGKILL is uncatchable, so the
    // worker publishes no completion and never acks — a stranded PEL entry.
    child.kill("SIGKILL");
    await waitForExit(child);
    console.log(
      `[demo] SIGKILLed ${workerId} mid-task — no completion will arrive`
    );

    // Bring up recovery: a healthy worker and the reclaimer.
    workerB = new WorkerConsumer(REDIS_URL, {
      heartbeat: {
        ttlSeconds: HEARTBEAT_TTL_SECONDS,
        intervalMs: HEARTBEAT_INTERVAL_MS,
      },
      backoff: {
        baseDelayMs: RETRY_BASE_DELAY_MS,
        maxDelayMs: RETRY_MAX_DELAY_MS,
      },
    });
    const workerBLoop = workerB.start();
    await reclaimer.start();
    await promoter.start();

    const result = await runPromise;

    workerB.requestShutdown();
    await workerBLoop;
    await workerB.disconnect();
    workerB = null;
    await reclaimer.disconnect();

    console.log("\n[demo] event log:");
    for (const event of result.events) {
      console.log(`  ${event.type.padEnd(10)} ${event.taskIds.join(", ")}`);
    }

    const stats = reclaimer.getStats();
    const promoterStats = promoter.getStats();
    console.log(
      `\n[demo] status=${result.status} ` +
        `completed=[${result.completed.join(", ")}] ` +
        `failed=[${result.failed.join(", ")}]`
    );
    console.log(
      `[demo] reclaimer stats: reclaimed=${stats.reclaimed} DLQ=${stats.deadLettered}`
    );
    console.log(
      `[demo] promoter stats: promoted=${promoterStats.promoted} dropped=${promoterStats.dropped}`
    );

    if (result.status !== "COMPLETED") {
      console.error(`\n[demo] FAILED: expected COMPLETED, got ${result.status}`);
      process.exit(1);
    }
    if (stats.reclaimed < 1) {
      console.error(
        "\n[demo] FAILED: nothing was reclaimed — the failover path was not exercised"
      );
      process.exit(1);
    }
    if (!result.completed.includes("long_task")) {
      console.error("\n[demo] FAILED: long_task never completed after reclaim");
      process.exit(1);
    }

    console.log(
      "\n[demo] GAP A CLOSED: a SIGKILLed worker's task was reclaimed, re-run, " +
        "and the workflow advanced instead of deadlocking"
    );
    process.exit(0);
  } finally {
    if (workerB) {
      workerB.requestShutdown();
      await workerB.disconnect();
    }
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
      await waitForExit(child);
    }
    await promoter.disconnect();
    await completions.disconnect();
    await dispatcher.disconnect();
  }
}

main().catch((err: unknown) => {
  console.error("[demo] fatal:", err);
  process.exit(1);
});
