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