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
/** `XREVRANGE` returns the same `[entryId, flatFields]` tuples as `XRANGE`. */
type XRevRangeReply = null | Array<[string, string[]]>;

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
 * Phase 4. The cursor is owned by the orchestrator instead — it saves it to the
 * state hash once a batch is applied, and `seek`s back to it on resume, so an
 * interrupted run replays only what it had not yet applied. Replay is always
 * safe because the orchestrator deduplicates completions.
 */
export class RedisCompletionSource implements CompletionSource {
  private readonly redis: Redis;
  private cursorPosition = "0";

  constructor(redisUrl?: string) {
    const url = redisUrl || process.env.REDIS_URL || DEFAULT_REDIS_URL;
    this.redis = new Redis(url);
  }

  /** Position after the most recent read; persist it once that batch is applied. */
  cursor(): string {
    return this.cursorPosition;
  }

  /** Resume from a persisted position instead of the start of the stream. */
  seek(cursor: string): void {
    this.cursorPosition = cursor;
  }

  /**
   * Position of the newest entry currently in the stream, or `"0"` when the
   * stream is missing or empty. Read with `XREVRANGE` rather than `XINFO STREAM`
   * because a missing key is an empty reply, not an error.
   */
  async headCursor(): Promise<string> {
    const reply = (await this.redis.call(
      "XREVRANGE",
      COMPLETION_STREAM,
      "+",
      "-",
      "COUNT",
      "1"
    )) as XRevRangeReply;

    if (!reply || reply.length === 0) return "0";
    return reply[0][0];
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
      this.cursorPosition
    )) as XReadReply;

    if (!reply || reply.length === 0) {
      return null;
    }

    const completions: TaskCompletion[] = [];

    for (const [, entries] of reply) {
      for (const [entryId, flatFields] of entries) {
        this.cursorPosition = entryId;
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
