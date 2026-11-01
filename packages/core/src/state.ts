/**
 * Pure workflow state machine. No Redis, network or filesystem imports — the
 * reducer is deterministic and fully testable in isolation, so `@cadence/core`
 * stays pure while owning the lifecycle rules.
 *
 * The orchestrator feeds it events as it dispatches tasks and observes
 * completions, then persists the resulting snapshot through a `WorkflowStateStore`
 * port. Keeping the math and the transition rules here means the persisted shape
 * and the in-memory shape cannot silently drift.
 */
import {
  type TaskCompletion,
  type TaskDefinition,
  type TaskRunRecord,
  type TerminalWorkflowStatus,
  type WorkflowLifecycleState,
  type WorkflowStateSnapshot,
} from "./types.js";

/** States from which no further transition is allowed. */
export const TERMINAL_WORKFLOW_STATES: ReadonlySet<WorkflowLifecycleState> =
  new Set(["COMPLETED", "FAILED", "STALLED"]);

export function isTerminalWorkflowState(
  state: WorkflowLifecycleState
): boolean {
  return TERMINAL_WORKFLOW_STATES.has(state);
}

/**
 * Events the orchestrator applies, in order, over a run's lifetime.
 */
export type WorkflowStateEvent =
  | { type: "WORKFLOW_STARTED"; at: number }
  | { type: "TASK_DISPATCHED"; taskId: string; attempt: number; at: number }
  | { type: "TASK_COMPLETED"; completion: TaskCompletion; at: number }
  | { type: "WORKFLOW_FINISHED"; status: TerminalWorkflowStatus; at: number };

/**
 * Fresh snapshot for a workflow that has not started: `PENDING`, with a record
 * per declared task so the shape is stable from the first write.
 */
export function initialWorkflowState(
  workflowId: string,
  tasks: TaskDefinition[],
  now: number
): WorkflowStateSnapshot {
  const records: Record<string, TaskRunRecord> = {};
  for (const task of tasks) {
    records[task.id] = {
      taskId: task.id,
      status: "PENDING",
      attempt: 0,
      dispatchedAt: 0,
    };
  }

  return {
    workflowId,
    state: "PENDING",
    startedAt: now,
    updatedAt: now,
    failed: [],
    tasks: records,
  };
}

/**
 * Apply one event, returning a NEW snapshot (never mutates the input).
 *
 * Transitions are guarded so the machine is monotonic and idempotent:
 * - `PENDING -> RUNNING` happens once; a later `WORKFLOW_STARTED` is ignored.
 * - Any event after a terminal state is ignored.
 * - A completion for an already-terminal task, or for a task the workflow never
 *   declared, is ignored — matching the orchestrator's idempotent handling of
 *   at-least-once duplicates.
 */
export function reduceWorkflowState(
  snapshot: WorkflowStateSnapshot,
  event: WorkflowStateEvent
): WorkflowStateSnapshot {
  switch (event.type) {
    case "WORKFLOW_STARTED": {
      if (snapshot.state !== "PENDING") return snapshot;
      return { ...snapshot, state: "RUNNING", updatedAt: event.at };
    }

    case "TASK_DISPATCHED": {
      if (isTerminalWorkflowState(snapshot.state)) return snapshot;
      const record = snapshot.tasks[event.taskId];
      if (!record) return snapshot;

      return {
        ...snapshot,
        updatedAt: event.at,
        tasks: {
          ...snapshot.tasks,
          [event.taskId]: {
            ...record,
            status: "RUNNING",
            attempt: event.attempt,
            dispatchedAt: event.at,
          },
        },
      };
    }

    case "TASK_COMPLETED": {
      if (isTerminalWorkflowState(snapshot.state)) return snapshot;

      const { completion } = event;
      const record = snapshot.tasks[completion.taskId];
      if (!record) return snapshot;
      if (record.status === "COMPLETED" || record.status === "FAILED") {
        return snapshot; // duplicate completion — already resolved
      }

      const updated: TaskRunRecord = {
        ...record,
        status: completion.status,
        attempt: completion.attempt,
        completedAt: event.at,
        durationMs: completion.durationMs,
        latencyMs: record.dispatchedAt
          ? event.at - record.dispatchedAt
          : completion.durationMs,
        exitCode: completion.exitCode,
        timedOut: completion.timedOut,
        error: completion.error,
      };

      const isNewFailure =
        completion.status === "FAILED" &&
        !snapshot.failed.includes(completion.taskId);

      return {
        ...snapshot,
        updatedAt: event.at,
        failed: isNewFailure
          ? [...snapshot.failed, completion.taskId]
          : snapshot.failed,
        tasks: { ...snapshot.tasks, [completion.taskId]: updated },
      };
    }

    case "WORKFLOW_FINISHED": {
      if (isTerminalWorkflowState(snapshot.state)) return snapshot;
      return {
        ...snapshot,
        state: event.status,
        updatedAt: event.at,
        completedAt: event.at,
      };
    }
  }
}
