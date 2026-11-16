/**
 * Pure idempotency contract.
 *
 * Delivery in Cadence is at-least-once, so the *same* task attempt can arrive
 * twice: the reclaimer re-publishes a stranded entry, and a retry can be
 * scheduled by both the retry ZSET and the reclaimer. Every attempt therefore
 * carries a deterministic invocation id, and the worker claims that id in Redis
 * before running anything — the first delivery wins, later ones are no-ops.
 *
 * This module has no Redis, network or filesystem imports, so `@cadence/core`
 * stays pure while the id format, the claim key and the acquire/duplicate
 * decision live in one place instead of being re-derived at each call site.
 */

/**
 * How long an invocation claim survives without being renewed, in seconds.
 * Long enough that a late duplicate (a slow reclaimer sweep, a backed-off
 * retry) still hits the claim; short enough that the keys cannot accumulate
 * forever.
 */
export const INVOCATION_TTL_SECONDS = 86_400;

/**
 * Deterministic identifier for one attempt of one task:
 * `run_<workflowId>_<taskId>_<attempt>`. Shared so the runner (which reports it
 * on the result) and the worker (which claims it) cannot disagree.
 */
export function buildInvocationId(
  workflowId: string,
  taskId: string,
  attempt: number
): string {
  return `run_${workflowId}_${taskId}_${attempt}`;
}

/** Redis key holding the claim for an invocation. */
export function invocationKey(invocationId: string): string {
  return `invocation:${invocationId}`;
}

export type InvocationClaim = "ACQUIRE" | "DUPLICATE";

/**
 * Decide whether a delivery may run its task, from the reply to
 * `SET <key> <workerId> EX <ttl> NX`.
 *
 * Redis (and ioredis) return `"OK"` when the key was absent — this delivery won
 * the claim — and `null` when it was already set, meaning another delivery owns
 * the invocation. Pinned here, and unit-tested, because it is exactly the kind
 * of implicit convention that fails silently elsewhere in this project.
 */
export function decideInvocationClaim(setReply: string | null): InvocationClaim {
  return setReply === null ? "DUPLICATE" : "ACQUIRE";
}
