import { Redis } from "ioredis";
import {
  TaskRunner,
  type TaskDefinition,
  type TaskExecutionResult,
} from "@cadence/core";

const TASKS_STREAM = "cadence:tasks";
const GROUP_NAME = "cadence:workers";
const DLQ_STREAM = "cadence:tasks:dlq";
// 127.0.0.1 rather than localhost: on Windows the Redis service binds IPv4 only,
// and Node resolves localhost verbatim, so "localhost" can dial ::1 and fail.
const DEFAULT_REDIS_URL = "redis://127.0.0.1:6379";

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
  const record: Record<string, string> = {};
  for (let i = 0; i + 1 < flat.length; i += 2) {
    record[flat[i]] = flat[i + 1];
  }
  return record;
}

export class WorkerConsumer {
  private redis: Redis;
  private workerId: string;
  private runner: TaskRunner;
  private shutdownRequested = false;

  constructor(redisUrl?: string) {
    const url = redisUrl || process.env.REDIS_URL || DEFAULT_REDIS_URL;
    this.redis = new Redis(url);
    this.workerId = `worker_${process.pid}_${Date.now()}`;
    this.runner = new TaskRunner();
  }

  /**
   * Start the long-polling consumer loop.
   * Blocks on XREADGROUP and dispatches tasks until shutdown is signaled.
   */
  async start(): Promise<void> {
    console.log(`[worker:${this.workerId}] Starting consumer (group=${GROUP_NAME})`);

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

    console.log(`[worker:${this.workerId}] Consumer loop exiting`);
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

    console.log(
      `[worker:${this.workerId}] Running task=${task.id} attempt=${attempt} workflow=${workflowId}`
    );

    const result = await this.execute(task, attempt, workflowId);

    if (result.status === "COMPLETED") {
      console.log(
        `[worker:${this.workerId}] Task ${task.id} completed (exit=${result.exitCode})`
      );
      await this.safeXack(messageId);
      return;
    }

    // Publish the follow-up BEFORE acknowledging. If the process dies between
    // the publish and the ack, the entry is still pending in the PEL and can be
    // recovered. Acknowledging first would lose the task outright.
    if (attempt < maxRetries) {
      console.log(
        `[worker:${this.workerId}] Task ${task.id} failed (exit=${result.exitCode}), ` +
          `retrying attempt=${attempt + 1}/${maxRetries}`
      );
      await this.redispatch(task, workflowId, attempt + 1);
    } else {
      console.log(
        `[worker:${this.workerId}] Task ${task.id} exhausted retries (${maxRetries}), routing to DLQ`
      );
      await this.routeToDlq(task, workflowId, attempt, result);
    }

    await this.safeXack(messageId);
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
        invocationId: `run_${workflowId}_${task.id}_${attempt}`,
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

  private async safeXack(messageId: string): Promise<void> {
    // Guard: only ack if we have a valid message id.
    if (messageId) {
      await this.redis.xack(TASKS_STREAM, GROUP_NAME, messageId);
    }
  }

  private async redispatch(
    task: TaskDefinition,
    workflowId: string,
    nextAttempt: number
  ): Promise<void> {
    await this.redis.xadd(
      TASKS_STREAM,
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
      String(nextAttempt)
    );
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
