import { Redis } from "ioredis";
import {
  DEFAULT_REDIS_URL,
  TASKS_STREAM as STREAM_KEY,
  WORKER_GROUP as GROUP_NAME,
  type TaskDefinition,
} from "@cadence/core";

export class TaskDispatcher {
  private redis: Redis;

  constructor(redisUrl?: string) {
    this.redis = new Redis(redisUrl || process.env.REDIS_URL || DEFAULT_REDIS_URL);
  }

  /**
   * Ensure the consumer group exists on the task stream.
   * Safely ignores BUSYGROUP error if the group is already present.
   */
  async ensureGroup(): Promise<void> {
    try {
      await this.redis.xgroup("CREATE", STREAM_KEY, GROUP_NAME, "0", "MKSTREAM");
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      if (!message.includes("BUSYGROUP")) {
        throw err;
      }
      // Group already exists — this is expected on restart.
    }
  }

  /**
   * Publish executable task payloads to the `cadence:tasks` Redis Stream.
   *
   * @param workflowId  Unique identifier for the workflow run.
   * @param tasks       Array of task definitions to dispatch.
   * @param attempt     Execution attempt number (1-indexed). Defaults to 1.
   * @returns          Array of published stream entry IDs.
   */
  async dispatchTasks(
    workflowId: string,
    tasks: TaskDefinition[],
    attempt = 1
  ): Promise<string[]> {
    const ids: string[] = [];

    for (const task of tasks) {
      const entryId = await this.redis.xadd(
        STREAM_KEY,
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
        String(attempt)
      );
      if (entryId) {
        ids.push(entryId);
      }
    }

    return ids;
  }

  /**
   * Gracefully close the Redis connection.
   */
  async disconnect(): Promise<void> {
    // quit() returns "OK" | null synchronously; explicitly discard the value.
    void this.redis.quit();
  }
}
