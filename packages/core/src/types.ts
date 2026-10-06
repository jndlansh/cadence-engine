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

export type TaskExecutionStatus =
  | "PENDING"
  | "READY"
  | "RUNNING"
  | "COMPLETED"
  | "FAILED";

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