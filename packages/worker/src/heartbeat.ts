import { Redis } from "ioredis";
import {
  DEFAULT_REDIS_URL,
  HEARTBEAT_INTERVAL_MS,
  HEARTBEAT_TTL_SECONDS,
  heartbeatKey,
} from "@cadence/core";

export interface HeartbeatOptions {
  /** Renewal cadence in ms. Must be comfortably below the TTL. */
  intervalMs?: number;
  /** Seconds the heartbeat key survives without renewal. */
  ttlSeconds?: number;
}

/**
 * Renews an ephemeral `worker:<id>:heartbeat` key so the orchestrator's
 * reclaimer can distinguish a dead worker from a merely slow one.
 *
 * Owns its own Redis connection on purpose: the consumer's connection is
 * occupied by a blocking `XREADGROUP ... BLOCK 2000`, so sharing it would
 * starve the heartbeat and make a healthy worker look dead.
 */
export class WorkerHeartbeat {
  private readonly redis: Redis;
  private readonly key: string;
  private readonly intervalMs: number;
  private readonly ttlSeconds: number;
  private timer: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly workerId: string,
    redisUrl?: string,
    options?: HeartbeatOptions
  ) {
    const url = redisUrl || process.env.REDIS_URL || DEFAULT_REDIS_URL;
    this.redis = new Redis(url);
    this.key = heartbeatKey(workerId);
    this.intervalMs = options?.intervalMs ?? HEARTBEAT_INTERVAL_MS;
    this.ttlSeconds = options?.ttlSeconds ?? HEARTBEAT_TTL_SECONDS;
  }

  /** Write the first beat immediately, then renew on an interval. */
  async start(): Promise<void> {
    await this.beat();
    this.timer = setInterval(() => {
      void this.beat();
    }, this.intervalMs);
    // The heartbeat alone must not keep the event loop alive.
    this.timer.unref?.();
  }

  private async beat(): Promise<void> {
    try {
      await this.redis.set(this.key, "alive", "EX", this.ttlSeconds);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      // Never throw from a timer callback. A missed beat is tolerated: the key
      // simply expires, and the TTL is sized for several missed beats.
      console.error(`[worker:${this.workerId}] heartbeat failed:`, message);
    }
  }

  /**
   * Stop renewing, drop the key and release the connection.
   *
   * Deleting the key on a *graceful* stop lets the reclaimer reclaim this
   * worker's in-flight tasks immediately instead of waiting out the TTL. A hard
   * crash leaves the key behind, so it still expires on schedule.
   */
  async disconnect(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    try {
      await this.redis.del(this.key);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(`[worker:${this.workerId}] heartbeat cleanup failed:`, message);
    }
    // quit() returns "OK" | null synchronously; explicitly discard the value.
    void this.redis.quit();
  }
}
