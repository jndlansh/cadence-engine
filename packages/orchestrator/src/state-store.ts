import { Redis } from "ioredis";
import {
  DEFAULT_REDIS_URL,
  WorkflowStateSnapshotSchema,
  workflowStateKey,
  type WorkflowStateSnapshot,
} from "@cadence/core";

/**
 * Port: durable storage for a workflow's lifecycle snapshot. The orchestrator
 * depends on this interface, so the DAG walk stays unit-testable with a fake.
 */
export interface WorkflowStateStore {
  save(snapshot: WorkflowStateSnapshot): Promise<void>;
  load(workflowId: string): Promise<WorkflowStateSnapshot | null>;
  disconnect(): Promise<void>;
}

/**
 * Persists each workflow's snapshot as a Redis hash at `workflow:<id>:state`.
 *
 * A hash (not a JSON blob) so `HGETALL` shows the lifecycle scalars at a glance
 * and `HGET ... tasks` yields the per-task detail, which is what a human or the
 * Phase 6 CLI will actually want to read. `HSET` overwrites, so each save is a
 * single idempotent write — no delete-then-set window.
 */
export class RedisWorkflowStateStore implements WorkflowStateStore {
  private readonly redis: Redis;

  constructor(redisUrl?: string) {
    const url = redisUrl || process.env.REDIS_URL || DEFAULT_REDIS_URL;
    this.redis = new Redis(url);
  }

  async save(snapshot: WorkflowStateSnapshot): Promise<void> {
    await this.redis.hset(
      workflowStateKey(snapshot.workflowId),
      "workflowId",
      snapshot.workflowId,
      "state",
      snapshot.state,
      "startedAt",
      String(snapshot.startedAt),
      "updatedAt",
      String(snapshot.updatedAt),
      "completedAt",
      snapshot.completedAt === undefined ? "" : String(snapshot.completedAt),
      "failed",
      JSON.stringify(snapshot.failed),
      "tasks",
      JSON.stringify(snapshot.tasks)
    );
  }

  async load(workflowId: string): Promise<WorkflowStateSnapshot | null> {
    const raw = await this.redis.hgetall(workflowStateKey(workflowId));

    if (!raw || Object.keys(raw).length === 0) {
      return null;
    }

    let candidate: unknown;
    try {
      candidate = {
        workflowId: raw["workflowId"] ?? workflowId,
        state: raw["state"],
        startedAt: raw["startedAt"],
        updatedAt: raw["updatedAt"],
        completedAt: raw["completedAt"] ? raw["completedAt"] : undefined,
        failed: raw["failed"] ? JSON.parse(raw["failed"]) : [],
        tasks: raw["tasks"] ? JSON.parse(raw["tasks"]) : {},
      };
    } catch (err: unknown) {
      // Never return a half-parsed state silently — a corrupt hash must be loud.
      const message = err instanceof Error ? err.message : String(err);
      console.error(
        `[state] malformed persisted state for workflow ${workflowId}: ${message}`
      );
      return null;
    }

    const parsed = WorkflowStateSnapshotSchema.safeParse(candidate);
    if (!parsed.success) {
      const details = parsed.error.issues
        .map((issue) => `${issue.path.join(".")}: ${issue.message}`)
        .join("; ");
      console.error(
        `[state] persisted state for workflow ${workflowId} failed validation: ${details}`
      );
      return null;
    }

    return parsed.data;
  }

  async disconnect(): Promise<void> {
    // quit() returns "OK" | null synchronously; explicitly discard the value.
    void this.redis.quit();
  }
}
