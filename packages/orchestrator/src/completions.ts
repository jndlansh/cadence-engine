import { Redis } from "ioredis";
import { TaskCompletionSchema, type TaskCompletion } from "@cadence/core";
import type { CompletionSource } from "./orchestrator.js";

const COMPLETION_STREAM = "cadence:completions";
// 127.0.0.1 rather than localhost: the Windows Redis binds IPv4 only, and Node
// resolves `localhost` verbatim so it may dial ::1 and fail.
const DEFAULT_REDIS_URL = "redis://127.0.0.1:6379";

type StreamEntry = [string, string[]];
type XReadReply = null | Array<[string, StreamEntry[]]>;

/**
 * Redis returns stream entry fields as a flat [field, value, field, value]
 * array — never an object — so fold the pairs before indexing.
 */
function toRecord(flat: string[]): Record<string, string> {
  const record: Record<string, string> = {};
  for (let i = 0; i + 1 < flat.length; i += 2) {
    record[flat[i]] = flat[i + 1];
  }
  return record;
}

/**
 * Fold and validate one completion entry. Returns null (loudly) for anything
 * that does not satisfy the contract rather than throwing into the read loop.
 */
export function parseCompletionEntry(flatFields: string[]): TaskCompletion | null {
  const parsed = TaskCompletionSchema.safeParse(toRecord(flatFields));

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
