/**
 * Standalone worker process. Used by scripts/demo-failover.ts to have a real,
 * SIGKILL-able worker, and reusable for the Phase 6 multi-worker launcher.
 *
 * Env:
 *   REDIS_URL              default redis://127.0.0.1:6379
 *   HEARTBEAT_TTL_SECONDS  default 10
 *   HEARTBEAT_INTERVAL_MS  default 3000
 *
 * Prints `WORKER_READY id=<id>` once it starts, so a parent can wait for it.
 */
import { WorkerConsumer } from "../packages/worker/src/consumer.js";

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

async function main(): Promise<void> {
  const worker = new WorkerConsumer(REDIS_URL, {
    heartbeat: {
      ttlSeconds: Number(process.env.HEARTBEAT_TTL_SECONDS ?? "10"),
      intervalMs: Number(process.env.HEARTBEAT_INTERVAL_MS ?? "3000"),
    },
  });

  worker.installShutdownHandlers();
  console.log(`WORKER_READY id=${worker.getWorkerId()}`);

  await worker.start();
}

main().catch((err: unknown) => {
  console.error("[worker-node] fatal:", err);
  process.exit(1);
});
