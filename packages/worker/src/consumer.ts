import { Redis } from "ioredis";
import { TaskRunner, type TaskDefinition } from "@cadence/core";

const TASKS_STREAM = "cadence:tasks";
const GROUP_NAME = "cadence:workers";
const DLQ_STREAM = "cadence:tasks:dlq";
const DEFAULT_REDIS_URL = "redis://localhost:6379";

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
      // Malformed entry — acknowledge and discard.
      await this.safeXack(messageId);
      return;
    }

    const task = buildTaskDefinition(payload);
    const attempt = Number(payload.attempt);
    const maxRetries = Number(payload.retries);
    const workflowId = payload.workflowId;

    console.log(
      `[worker:${this.workerId}] Running task=${task.id} attempt=${attempt} workflow=${workflowId}`
    );

    try {
      const result = await this.runner.run(task, attempt, workflowId);

      if (result.status === "COMPLETED") {
        console.log(
          `[worker:${this.workerId}] Task ${task.id} completed (exit=${result.exitCode})`
        );
        await this.safeXack(messageId);
        return;
      }

      // FAILED — decide between retry or DLQ.
      if (attempt < maxRetries) {
        console.log(
          `[worker:${this.workerId}] Task ${task.id} failed (exit=${result.exitCode}), ` +
            `retrying attempt=${attempt + 1}/${maxRetries}`
        );
        await this.safeXack(messageId);
        await this.redispatch(task, workflowId, attempt + 1);
      } else {
        console.log(
          `[worker:${this.workerId}] Task ${task.id} exhausted retries (${maxRetries}), routing to DLQ`
        );
        await this.safeXack(messageId);
        await this.routeToDlq(task, workflowId, attempt, result);
      }
    } catch (err: unknown) {
      // Unexpected execution error — treat as failed attempt.
      console.error(`[worker:${this.workerId}] Task ${task.id} threw:`, err);
      await this.safeXack(messageId);
      if (attempt < maxRetries) {
        await this.redispatch(task, workflowId, attempt + 1);
      } else {
        await this.routeToDlq(
          task,
          workflowId,
          attempt,
          {
            exitCode: 1,
            stdout: "",
            stderr: err instanceof Error ? err.message : String(err),
            timedOut: false,
            durationMs: 0,
            error: err instanceof Error ? err : new Error(String(err)),
          }
        );
      }
    }
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
