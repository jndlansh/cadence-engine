import { Redis } from "ioredis";
import { DEFAULT_REDIS_URL } from "@cadence/core";

/**
 * Fail fast with a single, catchable error when Redis is unreachable, instead
 * of letting every downstream client retry 20 times and spray
 * "Unhandled error event" lines at the user.
 *
 * Uses `lazyConnect` + `retryStrategy: () => null` so the failure happens once,
 * immediately, with the real `ECONNREFUSED` reason.
 */
export async function pingRedis(
  redisUrl?: string,
  timeoutMs = 3000
): Promise<void> {
  const url = redisUrl || process.env.REDIS_URL || DEFAULT_REDIS_URL;

  const redis = new Redis(url, {
    lazyConnect: true,
    connectTimeout: timeoutMs,
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
    retryStrategy: () => null,
  });

  // ioredis reports the real cause (e.g. ECONNREFUSED) on its 'error' event,
  // while the awaited call rejects with a vaguer "Connection is closed." Capture
  // the former and surface it — and swallow the event so it is not logged.
  let lastError: Error | null = null;
  redis.on("error", (err: Error) => {
    lastError = err;
  });

  try {
    await redis.connect();
    await redis.ping();
  } catch (err) {
    throw lastError ?? (err instanceof Error ? err : new Error(String(err)));
  } finally {
    redis.disconnect();
  }
}
