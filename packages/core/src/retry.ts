/**
 * Pure retry/backoff domain logic. No Redis, network or filesystem imports, so
 * `@cadence/core` stays pure and deterministic.
 *
 * A failed task is parked in the `RETRY_ZSET` sorted set — scored by the
 * epoch-ms at which it becomes due — and later promoted back onto the task
 * stream by the orchestrator. Both halves of that contract live here so the
 * delay math and the member encoding cannot drift apart silently.
 */
import { RetryPayloadSchema, type RetryPayload } from "./types.js";

export interface BackoffOptions {
  /** Delay before the first retry, in milliseconds. */
  baseDelayMs?: number;
  /** Upper bound on the delay, so the backoff cannot grow without limit. */
  maxDelayMs?: number;
}

export const DEFAULT_RETRY_BASE_DELAY_MS = 1000;
export const DEFAULT_RETRY_MAX_DELAY_MS = 60_000;

/**
 * Exponential backoff: `base * 2^(retryNumber-1)`, capped at `maxDelayMs`.
 *
 * @param retryNumber 1-based ordinal of the retry — 1 is the first retry, which
 *   waits one `baseDelayMs`. Values below 1 are treated as the first retry.
 *
 * The exponent is clamped before the shift so a large retry number cannot
 * overflow to `Infinity` and slip past the cap.
 */
export function computeBackoffDelayMs(
  retryNumber: number,
  options?: BackoffOptions
): number {
  const base = options?.baseDelayMs ?? DEFAULT_RETRY_BASE_DELAY_MS;
  const max = options?.maxDelayMs ?? DEFAULT_RETRY_MAX_DELAY_MS;
  const exponent = Math.max(0, Math.min(Math.floor(retryNumber) - 1, 30));
  return Math.min(base * 2 ** exponent, max);
}

/**
 * Serialize a retry payload into a stable ZSET member. Field order is fixed and
 * the attempt is included, so each attempt of a task is a distinct member, while
 * re-scheduling the same attempt is idempotent (ZADD simply updates its score).
 */
export function encodeRetryPayload(payload: RetryPayload): string {
  return JSON.stringify({
    workflowId: payload.workflowId,
    taskId: payload.taskId,
    command: payload.command,
    timeoutMs: payload.timeoutMs,
    retries: payload.retries,
    attempt: payload.attempt,
  });
}

/** Parse a ZSET member back into a payload, or null when it is malformed. */
export function decodeRetryPayload(member: string): RetryPayload | null {
  let raw: unknown;
  try {
    raw = JSON.parse(member);
  } catch {
    return null;
  }
  const parsed = RetryPayloadSchema.safeParse(raw);
  return parsed.success ? parsed.data : null;
}
