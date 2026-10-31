/**
 * Phase 3 smoke test: TaskDispatcher -> Redis Streams -> WorkerConsumer.
 *
 * Run with:  pnpm exec tsx scripts/smoke-phase3.ts
 *
 * This drives the real TaskDispatcher and WorkerConsumer against a live Redis.
 * It intentionally pins 127.0.0.1 instead of `localhost`: on Windows the
 * bundled Redis (Memurai) binds IPv4 only, while Node >=18 resolves
 * `localhost` verbatim and may try ::1 first.
 *
 * Verdict is read back out-of-band with memurai-cli / redis-cli:
 *   XPENDING cadence:tasks cadence:workers -> empty  (no orphans - the real signal)
 *   XLEN cadence:tasks            -> 4          (3 dispatched + 1 retry)
 *        NOTE: streams are append-only; XACK clears the PEL, it does NOT
 *        remove entries, so this count never drops to 0.
 *   XLEN cadence:tasks:dlq        -> 2          (fail + timeout routed)
 *   XRANGE cadence:tasks:dlq - +  -> inspect payloads
 */
import { TaskDispatcher } from "../packages/orchestrator/src/scheduler.js";
import { RetryPromoter } from "../packages/orchestrator/src/promoter.js";
import { WorkerConsumer } from "../packages/worker/src/consumer.js";

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";
const WORKFLOW_ID = "smoke_wf_1";

interface Scenario {
  id: string;
  command: string;
  retries: number;
  timeout_ms: number;
  expect: string;
}

const SCENARIOS: Scenario[] = [
  {
    id: "ok_echo",
    command: "echo hello-from-cadence",
    retries: 0,
    timeout_ms: 5000,
    expect: "COMPLETED then XACK",
  },
  {
    id: "fail_then_dlq",
    command: 'node -e "process.exit(3)"',
    retries: 2,
    timeout_ms: 5000,
    expect: "attempt 1 + 2 fail, then DLQ",
  },
  {
    id: "timeout_kill",
    command: 'node -e "setTimeout(function(){},5000)"',
    retries: 0,
    timeout_ms: 1000,
    expect: "SIGKILL after 1s, exit 124, then DLQ",
  },
];

const DRAIN_MS = 12_000;

async function main(): Promise<void> {
  const dispatcher = new TaskDispatcher(REDIS_URL);
  // A short backoff keeps the retry escalation observable within the drain
  // window. Real deployments use the package defaults (1s base, 60s cap).
  const worker = new WorkerConsumer(REDIS_URL, {
    backoff: { baseDelayMs: 500, maxDelayMs: 2000 },
  });
  const promoter = new RetryPromoter(REDIS_URL, { pollIntervalMs: 300 });

  await dispatcher.ensureGroup();
  console.log(`[smoke] group ensured; worker=${worker.getWorkerId()}`);

  const loop = worker.start();
  await promoter.start();

  for (const s of SCENARIOS) {
    console.log(`[smoke] will dispatch ${s.id} -> expect: ${s.expect}`);
  }

  const ids = await dispatcher.dispatchTasks(
    WORKFLOW_ID,
    SCENARIOS.map((s) => ({
      id: s.id,
      command: s.command,
      depends_on: [],
      retries: s.retries,
      timeout_ms: s.timeout_ms,
    }))
  );
  console.log(`[smoke] published ${ids.length} entries: ${ids.join(", ")}`);

  console.log(`[smoke] draining for ${DRAIN_MS}ms...`);
  await new Promise((resolve) => setTimeout(resolve, DRAIN_MS));

  console.log("[smoke] requesting worker shutdown");
  worker.requestShutdown();
  await loop;
  await promoter.disconnect();
  await worker.disconnect();
  await dispatcher.disconnect();
  console.log("[smoke] done - now inspect with memurai-cli");
  process.exit(0);
}

main().catch((err: unknown) => {
  console.error("[smoke] fatal:", err);
  process.exit(1);
});
