import { pingRedis } from "@cadence/orchestrator";

/** Exit code for an environment/precondition failure (e.g. Redis unreachable). */
export const ENV_ERROR = 4;

/** Injectable ping so the check is unit-testable without a live Redis. */
export type Ping = (redisUrl: string) => Promise<void>;

/**
 * Preflight: confirm Redis is reachable before any client, worker, or stream is
 * created. On failure, prints one actionable message and returns false.
 */
export async function ensureRedis(
  redisUrl: string,
  ping: Ping = pingRedis
): Promise<boolean> {
  try {
    await ping(redisUrl);
    return true;
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    console.error(`[cadence] cannot connect to Redis at ${redisUrl}`);
    console.error(`[cadence]   ${reason}`);
    console.error(
      "[cadence] start Redis (Memurai service, or `docker compose up -d redis`), " +
        "or pass --redis-url <url>"
    );
    return false;
  }
}
