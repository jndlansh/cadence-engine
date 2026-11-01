import { z } from "zod";

export const TaskSchema = z.object({
  id: z.string().min(1, "Task ID cannot be empty"),
  name: z.string().optional(),
  command: z.string().min(1, "Task command cannot be empty"),
  depends_on: z.array(z.string()).default([]),
  retries: z.number().int().nonnegative().default(0),
  timeout_ms: z.number().int().positive().default(30000),
});

export const WorkflowSchema = z.object({
  version: z.string().default("1.0"),
  workflow: z.string().min(1, "Workflow name cannot be empty"),
  tasks: z.array(TaskSchema).min(1, "Workflow must define at least one task"),
});

export type TaskDefinition = z.infer<typeof TaskSchema>;
export type WorkflowDefinition = z.infer<typeof WorkflowSchema>;

export const TaskExecutionStatusSchema = z.enum([
  "PENDING",
  "READY",
  "RUNNING",
  "COMPLETED",
  "FAILED",
]);

export type TaskExecutionStatus = z.infer<typeof TaskExecutionStatusSchema>;

/**
 * Result of a single task execution attempt.
 */
export interface TaskExecutionResult {
  /** The task ID that was executed. */
  taskId: string;
  /** Deterministic identifier: run_<workflowId>_<taskId>_<attempt>. */
  invocationId: string;
  /** Terminal execution status: COMPLETED or FAILED. */
  status: TaskExecutionStatus;
  /** Normalized process exit code (0 = success, non-zero = failure). */
  exitCode: number;
  /** Buffered stdout output. */
  stdout: string;
  /** Buffered stderr output. */
  stderr: string;
  /** True if the process was killed by the timeout guard. */
  timedOut: boolean;
  /** Wall-clock duration of the execution in milliseconds. */
  durationMs: number;
  /** Error if the process could not be spawned, otherwise null. */
  error: Error | null;
}

/**
 * Optional callbacks for streaming log output during execution.
 */
export interface TaskRunnerOptions {
  /** Invoked for each chunk of stdout produced by the child process. */
  onStdout?: (chunk: string) => void;
  /** Invoked for each chunk of stderr produced by the child process. */
  onStderr?: (chunk: string) => void;
}

/**
 * Decode a boolean that arrives as a stream string.
 *
 * Deliberately NOT `z.coerce.boolean()`: `Boolean("false")` is `true`, so a
 * worker writing `timedOut=false` would be read back as timed-out. Only the
 * exact strings `"true"` / `"false"` are accepted, and absent means `undefined`.
 */
const streamBoolean = z
  .enum(["true", "false"])
  .transform((v) => v === "true")
  .optional();

/**
 * Terminal outcome of a task, published by the worker to the completions stream
 * so the orchestrator can advance the DAG.
 *
 * Only terminal states appear here: the worker retries internally, so the
 * orchestrator never observes intermediate attempts. Numeric fields use
 * `z.coerce` because Redis stream fields always arrive as strings.
 */
export const TaskCompletionSchema = z.object({
  workflowId: z.string().min(1),
  taskId: z.string().min(1),
  status: z.enum(["COMPLETED", "FAILED"]),
  attempt: z.coerce.number().int().positive(),
  exitCode: z.coerce.number().int(),
  durationMs: z.coerce.number().int().nonnegative(),
  /** True when the timeout guard killed the process. Absent on older entries. */
  timedOut: streamBoolean,
  /** Failure detail (spawn error or captured stderr). Absent on success. */
  error: z.string().optional(),
});

export type TaskCompletion = z.infer<typeof TaskCompletionSchema>;

/**
 * A task waiting for a backed-off retry, stored as a member of the
 * `RETRY_ZSET` sorted set. Kept in core so the worker (producer) and the
 * orchestrator's promoter (consumer) cannot silently disagree on the shape.
 */
export const RetryPayloadSchema = z.object({
  workflowId: z.string().min(1),
  taskId: z.string().min(1),
  command: z.string().min(1),
  timeoutMs: z.coerce.number().int().positive(),
  retries: z.coerce.number().int().nonnegative(),
  attempt: z.coerce.number().int().positive(),
});

export type RetryPayload = z.infer<typeof RetryPayloadSchema>;

/**
 * Lifecycle of a whole workflow run. `STALLED` mirrors the orchestrator's stall
 * guard: no completion arrived within the timeout, which (before Phase 4) meant
 * a dead worker and now means something is badly wrong.
 */
export const WorkflowLifecycleStateSchema = z.enum([
  "PENDING",
  "RUNNING",
  "COMPLETED",
  "FAILED",
  "STALLED",
]);

export type WorkflowLifecycleState = z.infer<
  typeof WorkflowLifecycleStateSchema
>;

/** Terminal workflow states — once reached, no further transition applies. */
export type TerminalWorkflowStatus = "COMPLETED" | "FAILED" | "STALLED";

/**
 * Per-task record inside a workflow state snapshot: the last known status, the
 * attempt, and timing/outcome detail. Numeric/optional fields default sensibly
 * for a task that has not been dispatched yet.
 */
export const TaskRunRecordSchema = z.object({
  taskId: z.string().min(1),
  status: TaskExecutionStatusSchema,
  attempt: z.number().int().nonnegative(),
  dispatchedAt: z.number().int().nonnegative(),
  completedAt: z.number().int().nonnegative().optional(),
  /** Execution time reported by the worker's completion. */
  durationMs: z.number().int().nonnegative().optional(),
  /** Dispatch-to-completion wall time, including queue wait. */
  latencyMs: z.number().int().nonnegative().optional(),
  exitCode: z.number().int().optional(),
  timedOut: z.boolean().optional(),
  error: z.string().optional(),
});

export type TaskRunRecord = z.infer<typeof TaskRunRecordSchema>;

/**
 * Persisted snapshot of a workflow run. Held as a Redis hash
 * (`workflow:<id>:state`) so it is durable and inspectable with `HGETALL`.
 * Timestamps use `z.coerce` because values read back from a Redis hash are
 * strings, while in-memory snapshots hold real numbers.
 */
export const WorkflowStateSnapshotSchema = z.object({
  workflowId: z.string().min(1),
  state: WorkflowLifecycleStateSchema,
  startedAt: z.coerce.number().int().nonnegative(),
  updatedAt: z.coerce.number().int().nonnegative(),
  completedAt: z.coerce.number().int().nonnegative().optional(),
  failed: z.array(z.string()),
  tasks: z.record(z.string(), TaskRunRecordSchema),
});

export type WorkflowStateSnapshot = z.infer<typeof WorkflowStateSnapshotSchema>;