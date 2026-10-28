import { Redis } from "ioredis";
import {
  COMPLETION_STREAM,
  DEFAULT_REDIS_URL,
  TaskCompletionSchema,
  foldStreamFields,
  type TaskCompletion,
} from "@cadence/core";
import type { CompletionSource } from "./orchestrator.js";

type StreamEntry = [string, string[]];
type XReadReply = null | Array<[string, StreamEntry[]]>;

/**
 * Fold and validate one completion entry. Returns null (loudly) for anything
 * that does not satisfy the contract rather than throwing into the read loop.
 */
export function parseCompletionEntry(flatFields: string[]): TaskCompletion | null {
  const parsed = TaskCompletionSchema.safeParse(foldStreamFields(flatFields));

  if (!parsed.success) {
    const details = parsed.error.issues
      .map((issue) => `${issue.path.join(".")}: ${issue.message}`)
      .join("; ");
    console.error(
      `[orchestrator] malformed completion entry — skipped. ${details}`
    );
    return null;
  }

  return parsed.data;
}

/**
 * Reads the completions stream with a plain XREAD and an in-memory cursor.
 *
 * Deliberately not a consumer group: a group would accrue a Pending Entries
 * List that nothing ever drains, which is exactly the failure mode deferred to
 * Phase 4. Orchestrator state is in-memory anyway (Phase 5 decides persistence),
 * and the orchestrator deduplicates completions, so replaying from `0` is safe.
 */
export class RedisCompletionSource implements CompletionSource {
  private readonly redis: Redis;
  private cursor = "0";

  constructor(redisUrl?: string) {
    const url = redisUrl || process.env.REDIS_URL || DEFAULT_REDIS_URL;
    this.redis = new Redis(url);
  }

  async read(): Promise<TaskCompletion[] | null> {
    const reply = (await this.redis.call(
      "XREAD",
      "BLOCK",
      "2000",
      "COUNT",
      "64",
      "STREAMS",
      COMPLETION_STREAM,
      this.cursor
    )) as XReadReply;

    if (!reply || reply.length === 0) {
      return null;
    }

    const completions: TaskCompletion[] = [];

    for (const [, entries] of reply) {
      for (const [entryId, flatFields] of entries) {
        this.cursor = entryId;
        const completion = parseCompletionEntry(flatFields);
        if (completion) {
          completions.push(completion);
        }
      }
    }

    return completions;
  }

  async disconnect(): Promise<void> {
    // quit() returns "OK" | null synchronously; explicitly discard the value.
    void this.redis.quit();
  }
}
