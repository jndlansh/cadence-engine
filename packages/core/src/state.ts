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
    cursor: "0",
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

/**
 * Move the persisted completions cursor forward.
 *
 * The cursor is bookkeeping, not lifecycle, so it is deliberately NOT an event
 * in the reducer's union — but it still belongs here so every mutation of a
 * snapshot goes through `@cadence/core`. Returns the same snapshot when nothing
 * changed (so callers can persist only real movement) and never advances a
 * terminal run.
 */
export function withCursor(
  snapshot: WorkflowStateSnapshot,
  cursor: string
): WorkflowStateSnapshot {
  if (snapshot.cursor === cursor) return snapshot;
  if (isTerminalWorkflowState(snapshot.state)) return snapshot;
  return { ...snapshot, cursor };
}

/** A task that was dispatched but had not resolved when the run stopped. */
export interface InFlightTask {
  taskId: string;
  /** The attempt it was dispatched at — it is re-dispatched at the same one. */
  attempt: number;
}

export interface WorkflowResumePlan {
  /** `FRESH` starts from scratch; `RESUME` continues a persisted run. */
  mode: "FRESH" | "RESUME";
  /** Tasks already known terminal-completed, seeded so their children unblock. */
  completed: string[];
  /** Tasks that failed terminally before the run stopped. */
  failed: string[];
  /** Tasks still in flight — re-dispatched when the run continues. */
  inFlight: InFlightTask[];
}

/**
 * Decide what a run should pick up from a persisted snapshot.
 *
 * `RESUME` seeds the already-completed tasks (so their children are recognised
 * as unblocked even if they were never dispatched), carries over terminal
 * failures, and re-dispatches whatever was still RUNNING. Anything that was
 * `PENDING` is left to the normal wave dispatch.
 *
 * In-flight tasks are re-dispatched **at their persisted attempt**, never at
 * `attempt + 1`. That is what keeps resume idempotent: if the task had actually
 * finished and only its completion was lost, the worker's invocation claim turns
 * the re-dispatch into a no-op and the completion is replayed from the persisted
 * cursor — so the command never runs twice. The deliberate consequence is that a
 * task whose *worker died* stays suppressed at that attempt; recovering it is
 * the reclaimer's job (it re-publishes at `attempt + 1`), not resume's.
 */
export function planWorkflowResume(
  snapshot: WorkflowStateSnapshot | null,
  tasks: TaskDefinition[]
): WorkflowResumePlan {
  const fresh = (): WorkflowResumePlan => ({
    mode: "FRESH",
    completed: [],
    failed: [],
    inFlight: [],
  });

  // Nothing to continue: no snapshot, or the run already finished.
  if (!snapshot || isTerminalWorkflowState(snapshot.state)) return fresh();

  const plan = fresh();
  plan.mode = "RESUME";

  // Iterate the *declared* tasks, so a record left by a task that no longer
  // exists in the YAML is ignored rather than resurrected.
  for (const task of tasks) {
    const record = snapshot.tasks[task.id];
    if (!record) continue;

    if (record.status === "COMPLETED") {
      plan.completed.push(task.id);
    } else if (record.status === "FAILED") {
      plan.failed.push(task.id);
    } else if (record.status === "RUNNING") {
      plan.inFlight.push({
        taskId: task.id,
        attempt: record.attempt > 0 ? record.attempt : 1,
      });
    }
  }

  return plan;
}
