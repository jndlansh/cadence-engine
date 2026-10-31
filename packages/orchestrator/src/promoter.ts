import { Redis } from "ioredis";
import {
  DEFAULT_REDIS_URL,
  DLQ_STREAM,
  RETRY_ZSET,
  TASKS_STREAM,
  decodeRetryPayload,
} from "@cadence/core";

type ZRangeByScoreReply = string[] | null;

const DEFAULT_POLL_INTERVAL_MS = 1000;
const DEFAULT_BATCH_SIZE = 100;

export interface RetryPromoterOptions {
  /** How often to scan the retry set. */
  pollIntervalMs?: number;
  /** Max due members promoted per scan. */
  batchSize?: number;
}

export interface PromoteResult {
  /** Members that were due (score <= now) when the scan ran. */
  due: number;
  /** Task ids successfully moved back onto the task stream. */
  promoted: string[];
  /** Members discarded as malformed — routed to the DLQ, never silently. */
  dropped: string[];
}

/**
 * Drains due members of the `RETRY_ZSET` sorted set back onto `TASKS_STREAM`.
 *
 * The worker parks a failed task in the sorted set with a score equal to the
 * epoch-ms at which it becomes due, instead of re-publishing it immediately.
 * That yields exponential backoff without a worker sleeping on the task: the
 * worker acks its entry and moves on, and this promoter is what actually re-runs
 * the task once its delay has elapsed.
 *
 * Publish-before-remove: the retry is XADDed before it is ZREMmed. A crash in
 * between promotes it a second time (a duplicate, which is at-least-once)
 * rather than losing it — the same ordering rule as every other ack here.
 */
export class RetryPromoter {
  private readonly redis: Redis;
  private readonly promoterId: string;
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;
  private timer: ReturnType<typeof setInterval> | null = null;
  private promoting = false;
  private totalPromoted = 0;
  private totalDropped = 0;

  constructor(redisUrl?: string, options?: RetryPromoterOptions) {
    const url = redisUrl || process.env.REDIS_URL || DEFAULT_REDIS_URL;
    this.redis = new Redis(url);
    this.promoterId = `promoter_${process.pid}_${Date.now()}`;
    this.pollIntervalMs = options?.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
    this.batchSize = options?.batchSize ?? DEFAULT_BATCH_SIZE;
  }

  /** One scan of the retry set. Exposed for tests and single-shot promotion. */
  async promoteOnce(): Promise<PromoteResult> {
    const reply = (await this.redis.call(
      "ZRANGEBYSCORE",
      RETRY_ZSET,
      "-inf",
      String(Date.now()),
      "LIMIT",
      "0",
      String(this.batchSize)
    )) as ZRangeByScoreReply;

    const due = reply ?? [];
    const result: PromoteResult = {
      due: due.length,
      promoted: [],
      dropped: [],
    };

    for (const member of due) {
      const payload = decodeRetryPayload(member);

      if (!payload) {
        console.error(
          `[promoter:${this.promoterId}] malformed retry member — routing to DLQ: ${member}`
        );
        await this.redis.xadd(
          DLQ_STREAM,
          "*",
          "reason",
          "RETRY_MALFORMED",
          "member",
          member
        );
        await this.redis.zrem(RETRY_ZSET, member);
        result.dropped.push(member);
        continue;
      }

      // Publish before removing, exactly like every ack in this system.
      await this.redis.xadd(
        TASKS_STREAM,
        "*",
        "workflowId",
        payload.workflowId,
        "taskId",
        payload.taskId,
        "command",
        payload.command,
        "timeoutMs",
        String(payload.timeoutMs),
        "retries",
        String(payload.retries),
        "attempt",
        String(payload.attempt)
      );
      await this.redis.zrem(RETRY_ZSET, member);
      result.promoted.push(payload.taskId);
      console.log(
        `[promoter:${this.promoterId}] promoted ${payload.taskId} ` +
          `(attempt ${payload.attempt}) after backoff`
      );
    }

    this.totalPromoted += result.promoted.length;
    this.totalDropped += result.dropped.length;
    return result;
  }

  /** Cumulative promotion/DLQ totals since construction — for demos and metrics. */
  getStats(): { promoted: number; dropped: number } {
    return { promoted: this.totalPromoted, dropped: this.totalDropped };
  }

  /** Run an immediate scan, then keep scanning on an interval. */
  async start(): Promise<void> {
    const tick = async (): Promise<void> => {
      if (this.promoting) return;
      this.promoting = true;
      try {
        await this.promoteOnce();
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`[promoter:${this.promoterId}] scan failed:`, message);
      } finally {
        this.promoting = false;
      }
    };

    await tick();
    this.timer = setInterval(() => {
      void tick();
    }, this.pollIntervalMs);
    this.timer.unref?.();
    console.log(
      `[promoter:${this.promoterId}] started (poll=${this.pollIntervalMs}ms)`
    );
  }

  async stop(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async disconnect(): Promise<void> {
    await this.stop();
    // quit() returns "OK" | null synchronously; explicitly discard the value.
    void this.redis.quit();
  }

  /** Returns the unique promoter identifier assigned at construction time. */
  getPromoterId(): string {
    return this.promoterId;
  }
}
