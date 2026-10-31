/**
 * Shared Redis topology contract.
 *
 * These are plain strings and a pure fold function — no Redis, network or
 * filesystem imports — so `@cadence/core` stays pure while giving the
 * orchestrator and the worker a single source of truth for the key names they
 * must agree on. Duplicating a key name across packages fails *silently*
 * (e.g. a reclaimer reading the wrong heartbeat prefix never reclaims
 * anything), which is exactly the class of bug this project has been bitten by.
 */

/** Task stream. Workers consume it through `WORKER_GROUP`. */
export const TASKS_STREAM = "cadence:tasks";

/** Consumer group on `TASKS_STREAM`. */
export const WORKER_GROUP = "cadence:workers";

/** Dead letter stream for tasks that exhausted their retries. */
export const DLQ_STREAM = "cadence:tasks:dlq";

/** Terminal task outcomes published by workers for the orchestrator. */
export const COMPLETION_STREAM = "cadence:completions";

/**
 * Sorted set of tasks awaiting a backed-off retry. Members are encoded retry
 * payloads (`encodeRetryPayload`), scored by the epoch-ms time at which they
 * become due. The orchestrator's `RetryPromoter` moves due members back onto
 * `TASKS_STREAM`.
 */
export const RETRY_ZSET = "cadence:retries";

/**
 * Default Redis endpoint. `127.0.0.1`, never `localhost`: on Windows the Redis
 * service binds IPv4 only and Node resolves `localhost` verbatim, so it can
 * dial `::1` and fail. `REDIS_URL` overrides this at the call sites.
 */
export const DEFAULT_REDIS_URL = "redis://127.0.0.1:6379";

/** How long a worker heartbeat key survives without being renewed. */
export const HEARTBEAT_TTL_SECONDS = 10;

/** How often a worker renews its heartbeat (well inside the TTL). */
export const HEARTBEAT_INTERVAL_MS = 3000;

/**
 * Liveness marker key for a worker. The orchestrator's reclaimer treats a
 * missing key as "this worker is dead" and reclaims its pending tasks.
 */
export function heartbeatKey(workerId: string): string {
  return `worker:${workerId}:heartbeat`;
}

/**
 * Redis returns stream entry fields as a flat `[field, value, field, value]`
 * array — never an object — so indexing by key yields `undefined`. Fold the
 * pairs into a lookup record first. A dangling trailing field (no value) is
 * ignored, and a repeated field keeps its last occurrence.
 */
export function foldStreamFields(flat: string[]): Record<string, string> {
  const record: Record<string, string> = {};
  for (let i = 0; i + 1 < flat.length; i += 2) {
    record[flat[i]] = flat[i + 1];
  }
  return record;
}
