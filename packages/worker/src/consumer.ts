import { Redis } from "ioredis";
import {
  TaskRunner,
  COMPLETION_STREAM,
  DEFAULT_REDIS_URL,
  DLQ_STREAM,
  INVOCATION_TTL_SECONDS,
  RETRY_ZSET,
  TASKS_STREAM,
  WORKER_GROUP as GROUP_NAME,
  buildInvocationId,
  computeBackoffDelayMs,
  decideInvocationClaim,
  encodeInvocationOutcome,
  encodeRetryPayload,
  foldStreamFields,
  hasRetryBudget,
  invocationKey,
  invocationOutcomeKey,
  type BackoffOptions,
  type InvocationClaim,
  type TaskCompletion,
  type TaskDefinition,
  type TaskExecutionResult,
} from "@cadence/core";
import { WorkerHeartbeat, type HeartbeatOptions } from "./heartbeat.js";

export interface IdempotencyOptions {
  /** Seconds an invocation claim survives without renewal. */
  ttlSeconds?: number;
}

export interface WorkerConsumerOptions {
  /** Liveness-beacon tuning; the demo shrinks these to fail fast. */
  heartbeat?: HeartbeatOptions;
  /** Exponential-backoff tuning for failed-task retries. */
  backoff?: BackoffOptions;
  /** Idempotency claim tuning. */
  idempotency?: IdempotencyOptions;
}

interface StreamTaskPayload {
  workflowId: string;
  taskId: string;
  command: string;
  timeoutMs: string;
  retries: string;
  attempt: string;
}

function buildTaskDefinition(payload: StreamTaskPayload): TaskDefinition {
  return {
    id: payload.taskId,
    command: payload.command,
    depends_on: [],
    retries: Number(payload.retries),
    timeout_ms: Number(payload.timeoutMs),
  };
}

type StreamEntry = [string, string[]];
type XReadGroupReply = null | Array<[string, StreamEntry[]]>;

/**
 * Redis returns stream entry fields as a flat [field, value, field, value]
 * array — not as an object — so `fields["taskId"]` would always be undefined.
 * Fold the flat array into a lookup map.
 */
export function fieldsToRecord(flat: string[]): Record<string, string> {
  return foldStreamFields(flat);
}

export class WorkerConsumer {
  private redis: Redis;
  private workerId: string;
  private runner: TaskRunner;
  private readonly redisUrl: string;
  private readonly heartbeatOptions?: HeartbeatOptions;
  private readonly backoffOptions?: BackoffOptions;
  private readonly invocationTtlSeconds: number;
  private heartbeat: WorkerHeartbeat | null = null;
  private shutdownRequested = false;

  constructor(redisUrl?: string, options?: WorkerConsumerOptions) {
    this.redisUrl = redisUrl || process.env.REDIS_URL || DEFAULT_REDIS_URL;
    this.redis = new Redis(this.redisUrl);
    this.workerId = `worker_${process.pid}_${Date.now()}`;
    this.runner = new TaskRunner();
    this.heartbeatOptions = options?.heartbeat;
    this.backoffOptions = options?.backoff;
    this.invocationTtlSeconds =
      options?.idempotency?.ttlSeconds ?? INVOCATION_TTL_SECONDS;
  }

  /**
   * Start the long-polling consumer loop.
   * Blocks on XREADGROUP and dispatches tasks until shutdown is signaled.
   */
  async start(): Promise<void> {
    console.log(`[worker:${this.workerId}] Starting consumer (group=${GROUP_NAME})`);

    // Announce liveness so the reclaimer can tell a crash apart from a slow task.
    this.heartbeat = new WorkerHeartbeat(
      this.workerId,
      this.redisUrl,
      this.heartbeatOptions
    );
    await this.heartbeat.start();

    while (!this.shutdownRequested) {
      try {
        const messages = (await this.redis.call(
          "XREADGROUP",
          "GROUP",
          GROUP_NAME,
          this.workerId,
          "BLOCK",
          "2000",
          "COUNT",
          "1",
          "STREAMS",
          TASKS_STREAM,
          ">"
        )) as XReadGroupReply;

        if (!messages || messages.length === 0) {
          continue;
        }

        for (const [, entries] of messages) {
          if (this.shutdownRequested) break;

          for (const [entryId, flatFields] of entries) {
            if (this.shutdownRequested) break;
            await this.handleMessage(entryId, fieldsToRecord(flatFields));
          }
        }
      } catch (err: unknown) {
        if (this.shutdownRequested) break;
        const message = err instanceof Error ? err.message : String(err);
        // BLOCK with no messages within timeout returns null; ignore.
        // Reconnect/network errors should not kill the loop.
        if (!message.includes("null") && !message.includes("No such key")) {
          console.error(`[worker:${this.workerId}] XREADGROUP error:`, message);
        }
        // Back off briefly before retrying on error.
        await this.sleep(500);
      }
    }

    await this.stopHeartbeat();

    console.log(`[worker:${this.workerId}] Consumer loop exiting`);
  }

  private async stopHeartbeat(): Promise<void> {
    if (this.heartbeat) {
      await this.heartbeat.disconnect();
      this.heartbeat = null;
    }
  }

  private async handleMessage(
    messageId: string,
    fields: Record<string, string>
  ): Promise<void> {
    // Parse the payload out of the stream entry fields.
    const payload = {
      workflowId: fields["workflowId"] ?? "",
      taskId: fields["taskId"] ?? "",
      command: fields["command"] ?? "",
      timeoutMs: fields["timeoutMs"] ?? "30000",
      retries: fields["retries"] ?? "0",
      attempt: fields["attempt"] ?? "1",
    };

    if (!payload.taskId || !payload.command) {
      await this.quarantineMalformed(messageId, fields);
      return;
    }

    const task = buildTaskDefinition(payload);
    const attempt = Number(payload.attempt);
    const maxRetries = Number(payload.retries);
    const workflowId = payload.workflowId;
    const invocationId = buildInvocationId(workflowId, task.id, attempt);

    // Idempotency guard. Delivery is at-least-once, so this exact attempt can
    // arrive twice — a reclaimer re-publish, or a retry scheduled by both the
    // retry ZSET and the reclaimer. Claiming the invocation before running
    // means only the first delivery executes; later ones are no-ops. Claiming
    // *before* the command is deliberate: if the process dies after the claim,
    // the entry stays pending and is recovered, whereas claiming after the
    // command would let a duplicate re-run a side effect that already happened.
    if ((await this.claimInvocation(invocationId)) === "DUPLICATE") {
      console.warn(
        `[worker:${this.workerId}] duplicate delivery of ${invocationId} ` +
          `— already claimed, skipping execution`
      );
      // Whoever holds the claim publishes the outcome, so acking this copy is
      // safe; leaving it pending would only make the reclaimer reclaim it again.
      await this.safeXack(messageId);
      return;
    }

    console.log(
      `[worker:${this.workerId}] Running task=${task.id} attempt=${attempt} workflow=${workflowId}`
    );

    const result = await this.execute(task, attempt, workflowId);

    // Record a terminal outcome BEFORE routing it. If this process dies before
    // the entry is acked, the reclaimer resolves the task from this record
    // instead of running the command again — which matters because a reclaim
    // re-publishes under a *new* invocation id that no claim can suppress. A
    // failure that still has retry budget is deliberately not recorded (see
    // core/src/invocation.ts): replaying it would abort a workflow that is
    // about to retry.
    if (result.status === "COMPLETED" || !hasRetryBudget(attempt, maxRetries)) {
      await this.recordOutcome(invocationId, result);
    }

    if (result.status === "COMPLETED") {
      console.log(
        `[worker:${this.workerId}] Task ${task.id} completed (exit=${result.exitCode})`
      );
      await this.publishCompletion(task, workflowId, attempt, result);
      await this.safeXack(messageId);
      return;
    }

    // Publish the follow-up BEFORE acknowledging. If the process dies between
    // the publish and the ack, the entry is still pending in the PEL and can be
    // recovered. Acknowledging first would lose the task outright.
    if (hasRetryBudget(attempt, maxRetries)) {
      // Park the retry in the retry ZSET with a due-time score instead of
      // re-publishing immediately: the promoter re-dispatches it once the
      // exponential backoff has elapsed, so a hard-failing task cannot hammer
      // the queue. This ZADD is the publish half of publish-before-ack.
      const delayMs = computeBackoffDelayMs(attempt, this.backoffOptions);
      console.log(
        `[worker:${this.workerId}] Task ${task.id} failed (exit=${result.exitCode}), ` +
          `retry attempt=${attempt + 1}/${maxRetries} scheduled in ${delayMs}ms`
      );
      await this.scheduleRetry(task, workflowId, attempt + 1, delayMs);
    } else {
      console.log(
        `[worker:${this.workerId}] Task ${task.id} exhausted retries (${maxRetries}), routing to DLQ`
      );
      await this.routeToDlq(task, workflowId, attempt, result);
      await this.publishCompletion(task, workflowId, attempt, result);
    }

    await this.safeXack(messageId);
  }

  /**
   * Persist the terminal outcome of an attempt, so a stranded entry can be
   * resolved without running the command again. Best-effort: if the write fails
   * the run still completes — the record only matters if this process dies
   * before the ack, and without it the reclaimer falls back to re-dispatching.
   */
  private async recordOutcome(
    invocationId: string,
    result: TaskExecutionResult
  ): Promise<void> {
    try {
      await this.redis.set(
        invocationOutcomeKey(invocationId),
        encodeInvocationOutcome({
          status: result.status === "COMPLETED" ? "COMPLETED" : "FAILED",
          exitCode: result.exitCode,
          durationMs: result.durationMs,
          timedOut: result.timedOut,
          error: result.error
            ? result.error.message
            : result.status === "FAILED"
              ? result.stderr
              : "",
        }),
        "EX",
        this.invocationTtlSeconds
      );
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(
        `[worker:${this.workerId}] failed to record the outcome of ` +
          `${invocationId}: ${message}`
      );
    }
  }

  /**
   * Claim an invocation so it can run at most once. `SET ... NX` is atomic, so
   * exactly one delivery of a given attempt wins, even across workers. The
   * claim carries the owner's id and a TTL so a late duplicate is still
   * suppressed, while stale claims eventually expire instead of accumulating.
   */
  private async claimInvocation(
    invocationId: string
  ): Promise<InvocationClaim> {
    const reply = await this.redis.set(
      invocationKey(invocationId),
      this.workerId,
      "EX",
      this.invocationTtlSeconds,
      "NX"
    );
    return decideInvocationClaim(reply);
  }

  /**
   * Execute a task, converting an unexpected runner rejection into a FAILED
   * result. Scoping the try/catch to the runner alone keeps bookkeeping
   * (XADD/XACK) failures from being mistaken for task failures.
   */
  private async execute(
    task: TaskDefinition,
    attempt: number,
    workflowId: string
  ): Promise<TaskExecutionResult> {
    try {
      return await this.runner.run(task, attempt, workflowId);
    } catch (err: unknown) {
      const error = err instanceof Error ? err : new Error(String(err));
      console.error(`[worker:${this.workerId}] Task ${task.id} threw:`, error);
      return {
        taskId: task.id,
        invocationId: buildInvocationId(workflowId, task.id, attempt),
        status: "FAILED",
        exitCode: 1,
        stdout: "",
        stderr: error.message,
        timedOut: false,
        durationMs: 0,
        error,
      };
    }
  }

  /**
   * Handle an entry that is missing required fields: publish the raw entry to
   * the DLQ so it is visible and recoverable, then acknowledge. Never discard
   * silently — that is how the flat-array bug went unnoticed.
   */
  private async quarantineMalformed(
    messageId: string,
    fields: Record<string, string>
  ): Promise<void> {
    const raw = JSON.stringify(fields);
    console.error(
      `[worker:${this.workerId}] Malformed entry ${messageId} (missing taskId or command) ` +
        `— routing to DLQ. fields=${raw}`
    );

    await this.redis.xadd(
      DLQ_STREAM,
      "*",
      "reason",
      "MALFORMED_ENTRY",
      "messageId",
      messageId,
      "fields",
      raw
    );

    await this.safeXack(messageId);
  }

  /**
   * Publish the terminal outcome of a task so the orchestrator can advance the
   * DAG. Called before `safeXack`: a crash in between simply re-delivers the
   * task and produces a duplicate completion, which the orchestrator treats
   * idempotently.
   */
  private async publishCompletion(
    task: TaskDefinition,
    workflowId: string,
    attempt: number,
    result: TaskExecutionResult
  ): Promise<void> {
    const status: TaskCompletion["status"] =
      result.status === "COMPLETED" ? "COMPLETED" : "FAILED";

    await this.redis.xadd(
      COMPLETION_STREAM,
      "*",
      "workflowId",
      workflowId,
      "taskId",
      task.id,
      "status",
      status,
      "attempt",
      String(attempt),
      "exitCode",
      String(result.exitCode),
      "durationMs",
      String(result.durationMs),
      "timedOut",
      String(result.timedOut),
      "error",
      result.error
        ? result.error.message
        : result.status === "FAILED"
          ? result.stderr
          : ""
    );
  }

  private async safeXack(messageId: string): Promise<void> {
    // Guard: only ack if we have a valid message id.
    if (messageId) {
      await this.redis.xack(TASKS_STREAM, GROUP_NAME, messageId);
    }
  }

  /**
   * Park a failed task in the retry ZSET, scored by the epoch-ms at which it
   * becomes due. The orchestrator's `RetryPromoter` moves it back onto
   * `TASKS_STREAM` once due. The member encodes the whole payload so the
   * promoter can re-dispatch without any other state.
   */
  private async scheduleRetry(
    task: TaskDefinition,
    workflowId: string,
    nextAttempt: number,
    delayMs: number
  ): Promise<void> {
    const dueAt = Date.now() + delayMs;
    const member = encodeRetryPayload({
      workflowId,
      taskId: task.id,
      command: task.command,
      timeoutMs: task.timeout_ms,
      retries: task.retries,
      attempt: nextAttempt,
    });
    await this.redis.zadd(RETRY_ZSET, String(dueAt), member);
  }

  private async routeToDlq(
    task: TaskDefinition,
    workflowId: string,
    attempt: number,
    result: {
      exitCode: number;
      stdout: string;
      stderr: string;
      timedOut: boolean;
      durationMs: number;
      error: Error | null;
    }
  ): Promise<void> {
    await this.redis.xadd(
      DLQ_STREAM,
      "*",
      "workflowId",
      workflowId,
      "taskId",
      task.id,
      "command",
      task.command,
      "timeoutMs",
      String(task.timeout_ms),
      "retries",
      String(task.retries),
      "attempt",
      String(attempt),
      "exitCode",
      String(result.exitCode),
      "stdout",
      result.stdout,
      "stderr",
      result.stderr,
      "timedOut",
      String(result.timedOut),
      "durationMs",
      String(result.durationMs),
      "error",
      result.error ? result.error.message : ""
    );
  }

  /**
   * Signal the consumer loop to stop after finishing the current message.
   */
  requestShutdown(): void {
    this.shutdownRequested = true;
  }

  /**
   * Register OS signal handlers for graceful shutdown.
   */
  installShutdownHandlers(): void {
    const handle = async (signal: string) => {
      console.log(`[worker:${this.workerId}] Received ${signal}, shutting down`);
      this.requestShutdown();
      await this.disconnect();
      process.exit(0);
    };

    process.once("SIGINT", () => handle("SIGINT"));
    process.once("SIGTERM", () => handle("SIGTERM"));
  }

  /**
   * Gracefully close the Redis connection.
   */
  async disconnect(): Promise<void> {
    await this.stopHeartbeat();
    // quit() returns "OK" | null synchronously; explicitly discard the value.
    void this.redis.quit();
  }

  /**
   * Returns the unique worker identifier assigned at construction time.
   */
  getWorkerId(): string {
    return this.workerId;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
