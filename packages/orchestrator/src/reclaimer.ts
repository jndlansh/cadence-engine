import { Redis } from "ioredis";
import {
  COMPLETION_STREAM,
  DEFAULT_REDIS_URL,
  DLQ_STREAM,
  TASKS_STREAM,
  WORKER_GROUP,
  foldStreamFields,
  heartbeatKey,
} from "@cadence/core";

/** XPENDING extended form: [entryId, consumer, idleMs, deliveryCount]. */
export type PendingEntry = [string, string, number, number];
type XPendingReply = null | PendingEntry[];
/** XCLAIM returns [entryId, flatFields] tuples (empty when nothing was claimed). */
type XClaimReply = null | Array<[string, string[]]>;

const DEFAULT_POLL_INTERVAL_MS = 5000;
const DEFAULT_MIN_IDLE_MS = 5000;
const DEFAULT_BATCH_SIZE = 100;

export interface ReclaimerOptions {
  /** How often to sweep the PEL. */
  pollIntervalMs?: number;
  /** Minimum entry idle time before it is a candidate (also passed to XCLAIM). */
  minIdleMs?: number;
  /** Max PEL entries examined per sweep. */
  batchSize?: number;
}

export interface SweepResult {
  /** Pending entries seen in the PEL this sweep. */
  examined: number;
  /** Entry ids reclaimed from dead workers and re-dispatched. */
  reclaimed: string[];
  /** Entry ids routed to the DLQ (malformed or retries exhausted). */
  deadLettered: string[];
}

/**
 * Background sweeper that closes GAP A: it finds tasks stranded by a dead
 * worker and makes them runnable again, so the orchestrator actually receives
 * a completion instead of deadlocking.
 *
 * Reclaim is liveness-gated rather than idle-only: an entry is only taken from
 * its owner once that owner's `worker:<id>:heartbeat` key has expired. A merely
 * slow task still holds a valid heartbeat, so it is never stolen and re-run in
 * parallel.
 *
 * Because `XREADGROUP ... >` only ever yields *never-delivered* entries, a
 * claimed entry is invisible to every worker. Reclaim therefore means
 * "re-publish as a fresh stream entry, then ack the stranded original" —
 * publish-before-ack, exactly like the worker's retry path.
 */
export class TaskReclaimer {
  private readonly redis: Redis;
  private readonly reclaimerId: string;
  private readonly pollIntervalMs: number;
  private readonly minIdleMs: number;
  private readonly batchSize: number;
  private timer: ReturnType<typeof setInterval> | null = null;
  private sweeping = false;
  private totalReclaimed = 0;
  private totalDeadLettered = 0;

  constructor(redisUrl?: string, options?: ReclaimerOptions) {
    const url = redisUrl || process.env.REDIS_URL || DEFAULT_REDIS_URL;
    this.redis = new Redis(url);
    this.reclaimerId = `reclaimer_${process.pid}_${Date.now()}`;
    this.pollIntervalMs = options?.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
    this.minIdleMs = options?.minIdleMs ?? DEFAULT_MIN_IDLE_MS;
    this.batchSize = options?.batchSize ?? DEFAULT_BATCH_SIZE;
  }

  /** One pass over the PEL. Exposed for tests and single-shot sweeps. */
  async sweepOnce(): Promise<SweepResult> {
    const pending = await this.listPending();
    const result: SweepResult = {
      examined: pending.length,
      reclaimed: [],
      deadLettered: [],
    };

    for (const [entryId, consumer, idleMs] of pending) {
      // A fresh entry may belong to a worker whose heartbeat write has not
      // landed yet; never act on one younger than the idle margin.
      if (Number(idleMs) < this.minIdleMs) continue;
      if (await this.isWorkerAlive(consumer)) continue;

      const fields = await this.claim(entryId);
      // Someone else (a returning worker, another reclaimer) resolved it first.
      if (!fields) continue;

      const workflowId = fields["workflowId"] ?? "";
      const taskId = fields["taskId"] ?? "";
      const command = fields["command"] ?? "";

      if (!workflowId || !taskId || !command) {
        await this.routeMalformedToDlq(entryId, fields);
        await this.ack(entryId);
        result.deadLettered.push(entryId);
        continue;
      }

      const currentAttempt = Number(fields["attempt"] ?? "1");
      const maxRetries = Number(fields["retries"] ?? "0");

      // Mirror the worker's budget exactly: republish only while the attempt
      // that just died was still inside the retry allowance.
      if (currentAttempt < maxRetries) {
        await this.republish(fields, currentAttempt + 1);
        result.reclaimed.push(entryId);
        console.warn(
          `[reclaimer] reclaimed ${taskId} from dead worker ${consumer} ` +
            `(idle ${idleMs}ms) — re-dispatched as attempt ${currentAttempt + 1}`
        );
      } else {
        // Give up, but never silently: DLQ it AND emit a terminal completion so
        // the orchestrator advances/fails instead of deadlocking on a task that
        // will never report back.
        await this.routeToDlq(fields, currentAttempt);
        await this.publishFailure(fields, currentAttempt);
        result.deadLettered.push(entryId);
        console.error(
          `[reclaimer] ${taskId} exhausted retries after worker death ` +
            `(attempt ${currentAttempt}/${maxRetries}) — DLQ + FAILED completion`
        );
      }

      await this.ack(entryId);
    }

    this.totalReclaimed += result.reclaimed.length;
    this.totalDeadLettered += result.deadLettered.length;
    return result;
  }

  /** Cumulative reclaim/DLQ totals since construction — for demos and metrics. */
  getStats(): { reclaimed: number; deadLettered: number } {
    return {
      reclaimed: this.totalReclaimed,
      deadLettered: this.totalDeadLettered,
    };
  }

  /** Run an immediate sweep, then keep sweeping on an interval. */
  async start(): Promise<void> {
    const tick = async (): Promise<void> => {
      if (this.sweeping) return;
      this.sweeping = true;
      try {
        await this.sweepOnce();
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`[reclaimer:${this.reclaimerId}] sweep failed:`, message);
      } finally {
        this.sweeping = false;
      }
    };

    await tick();
    this.timer = setInterval(() => {
      void tick();
    }, this.pollIntervalMs);
    this.timer.unref?.();
    console.log(
      `[reclaimer:${this.reclaimerId}] started ` +
        `(poll=${this.pollIntervalMs}ms minIdle=${this.minIdleMs}ms)`
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

  /** Returns the consumer name this reclaimer claims under. */
  getReclaimerId(): string {
    return this.reclaimerId;
  }

  /** Snapshot of the consumer group's PEL: [entryId, consumer, idleMs, deliveries]. */
  async listPending(): Promise<PendingEntry[]> {
    const reply = (await this.redis.call(
      "XPENDING",
      TASKS_STREAM,
      WORKER_GROUP,
      "-",
      "+",
      String(this.batchSize)
    )) as XPendingReply;
    return reply ?? [];
  }

  /** True while the worker's heartbeat key is present. */
  async isWorkerAlive(workerId: string): Promise<boolean> {
    const exists = await this.redis.exists(heartbeatKey(workerId));
    return exists === 1;
  }

  private async claim(entryId: string): Promise<Record<string, string> | null> {
    const reply = (await this.redis.call(
      "XCLAIM",
      TASKS_STREAM,
      WORKER_GROUP,
      this.reclaimerId,
      String(this.minIdleMs),
      entryId
    )) as XClaimReply;

    if (!reply || reply.length === 0) return null;
    const [, flat] = reply[0];
    return foldStreamFields(flat);
  }

  private async republish(
    fields: Record<string, string>,
    attempt: number
  ): Promise<void> {
    await this.redis.xadd(
      TASKS_STREAM,
      "*",
      "workflowId",
      fields["workflowId"] ?? "",
      "taskId",
      fields["taskId"] ?? "",
      "command",
      fields["command"] ?? "",
      "timeoutMs",
      fields["timeoutMs"] ?? "30000",
      "retries",
      fields["retries"] ?? "0",
      "attempt",
      String(attempt)
    );
  }

  private async routeMalformedToDlq(
    entryId: string,
    fields: Record<string, string>
  ): Promise<void> {
    console.error(
      `[reclaimer] reclaimed entry ${entryId} is malformed ` +
        `(missing workflowId/taskId/command) — routing to DLQ`
    );
    await this.redis.xadd(
      DLQ_STREAM,
      "*",
      "reason",
      "MALFORMED_ENTRY",
      "messageId",
      entryId,
      "fields",
      JSON.stringify(fields)
    );
  }

  private async routeToDlq(
    fields: Record<string, string>,
    attempt: number
  ): Promise<void> {
    await this.redis.xadd(
      DLQ_STREAM,
      "*",
      "reason",
      "RECLAIM_EXHAUSTED",
      "workflowId",
      fields["workflowId"] ?? "",
      "taskId",
      fields["taskId"] ?? "",
      "command",
      fields["command"] ?? "",
      "attempt",
      String(attempt),
      "retries",
      fields["retries"] ?? "0"
    );
  }

  private async publishFailure(
    fields: Record<string, string>,
    attempt: number
  ): Promise<void> {
    await this.redis.xadd(
      COMPLETION_STREAM,
      "*",
      "workflowId",
      fields["workflowId"] ?? "",
      "taskId",
      fields["taskId"] ?? "",
      "status",
      "FAILED",
      "attempt",
      String(attempt),
      "exitCode",
      "1",
      "durationMs",
      "0"
    );
  }

  private async ack(entryId: string): Promise<void> {
    await this.redis.xack(TASKS_STREAM, WORKER_GROUP, entryId);
  }
}
