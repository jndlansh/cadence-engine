import { describe, it, expect } from "vitest";
import {
  initialWorkflowState,
  isTerminalWorkflowState,
  reduceWorkflowState,
} from "./state.js";
import type { TaskCompletion, TaskDefinition } from "./types.js";

const TASKS: TaskDefinition[] = [
  { id: "a", command: "echo a", depends_on: [], retries: 0, timeout_ms: 1000 },
  {
    id: "b",
    command: "echo b",
    depends_on: ["a"],
    retries: 0,
    timeout_ms: 1000,
  },
];

function completion(
  taskId: string,
  overrides: Partial<TaskCompletion> = {}
): TaskCompletion {
  return {
    workflowId: "wf",
    taskId,
    status: "COMPLETED",
    attempt: 1,
    exitCode: 0,
    durationMs: 10,
    ...overrides,
  };
}

describe("initialWorkflowState", () => {
  it("starts PENDING with one record per declared task", () => {
    const state = initialWorkflowState("wf", TASKS, 1000);

    expect(state.state).toBe("PENDING");
    expect(state.startedAt).toBe(1000);
    expect(state.updatedAt).toBe(1000);
    expect(state.failed).toEqual([]);
    expect(Object.keys(state.tasks).sort()).toEqual(["a", "b"]);
    expect(state.tasks["a"]).toEqual({
      taskId: "a",
      status: "PENDING",
      attempt: 0,
      dispatchedAt: 0,
    });
  });
});

describe("reduceWorkflowState", () => {
  it("transitions PENDING -> RUNNING once", () => {
    let state = initialWorkflowState("wf", TASKS, 0);

    state = reduceWorkflowState(state, { type: "WORKFLOW_STARTED", at: 5 });
    expect(state.state).toBe("RUNNING");
    expect(state.updatedAt).toBe(5);

    // A second start is ignored (same object, no transition).
    expect(reduceWorkflowState(state, { type: "WORKFLOW_STARTED", at: 6 })).toBe(
      state
    );
  });

  it("records a dispatch as RUNNING with the attempt and dispatch time", () => {
    let state = initialWorkflowState("wf", TASKS, 0);
    state = reduceWorkflowState(state, {
      type: "TASK_DISPATCHED",
      taskId: "a",
      attempt: 1,
      at: 10,
    });

    expect(state.tasks["a"]).toMatchObject({
      status: "RUNNING",
      attempt: 1,
      dispatchedAt: 10,
    });
  });

  it("records a completion with execution time and end-to-end latency", () => {
    let state = initialWorkflowState("wf", TASKS, 0);
    state = reduceWorkflowState(state, {
      type: "TASK_DISPATCHED",
      taskId: "a",
      attempt: 1,
      at: 10,
    });
    state = reduceWorkflowState(state, {
      type: "TASK_COMPLETED",
      completion: completion("a", { durationMs: 40 }),
      at: 100,
    });

    expect(state.tasks["a"]).toMatchObject({
      status: "COMPLETED",
      completedAt: 100,
      durationMs: 40,
      latencyMs: 90,
      exitCode: 0,
    });
  });

  it("falls back to execution duration for latency when never dispatched", () => {
    let state = initialWorkflowState("wf", TASKS, 0);
    state = reduceWorkflowState(state, { type: "WORKFLOW_STARTED", at: 1 });
    state = reduceWorkflowState(state, {
      type: "TASK_COMPLETED",
      completion: completion("a", { durationMs: 7 }),
      at: 100,
    });

    expect(state.tasks["a"].latencyMs).toBe(7);
  });

  it("records a failure with error detail and appends it to failed", () => {
    let state = initialWorkflowState("wf", TASKS, 0);
    state = reduceWorkflowState(state, {
      type: "TASK_DISPATCHED",
      taskId: "a",
      attempt: 2,
      at: 10,
    });
    state = reduceWorkflowState(state, {
      type: "TASK_COMPLETED",
      completion: completion("a", {
        status: "FAILED",
        exitCode: 124,
        timedOut: true,
        error: "timed out",
      }),
      at: 50,
    });

    expect(state.tasks["a"]).toMatchObject({
      status: "FAILED",
      exitCode: 124,
      timedOut: true,
      error: "timed out",
    });
    expect(state.failed).toEqual(["a"]);
  });

  it("ignores a duplicate completion for an already-resolved task", () => {
    let state = initialWorkflowState("wf", TASKS, 0);
    state = reduceWorkflowState(state, {
      type: "TASK_DISPATCHED",
      taskId: "a",
      attempt: 1,
      at: 10,
    });
    state = reduceWorkflowState(state, {
      type: "TASK_COMPLETED",
      completion: completion("a"),
      at: 20,
    });

    const again = reduceWorkflowState(state, {
      type: "TASK_COMPLETED",
      completion: completion("a", { status: "FAILED" }),
      at: 30,
    });

    expect(again).toBe(state);
    expect(again.failed).toEqual([]);
  });

  it("ignores a completion for a task the workflow never declared", () => {
    let state = initialWorkflowState("wf", TASKS, 0);
    state = reduceWorkflowState(state, { type: "WORKFLOW_STARTED", at: 1 });

    expect(
      reduceWorkflowState(state, {
        type: "TASK_COMPLETED",
        completion: completion("ghost"),
        at: 2,
      })
    ).toBe(state);
  });

  it("finishes with the given terminal status and stamps completedAt", () => {
    let state = initialWorkflowState("wf", TASKS, 0);
    state = reduceWorkflowState(state, { type: "WORKFLOW_STARTED", at: 1 });
    state = reduceWorkflowState(state, {
      type: "WORKFLOW_FINISHED",
      status: "FAILED",
      at: 99,
    });

    expect(state.state).toBe("FAILED");
    expect(state.completedAt).toBe(99);
    expect(isTerminalWorkflowState(state.state)).toBe(true);
  });

  it("ignores events after a terminal state", () => {
    let state = initialWorkflowState("wf", TASKS, 0);
    state = reduceWorkflowState(state, { type: "WORKFLOW_STARTED", at: 1 });
    state = reduceWorkflowState(state, {
      type: "WORKFLOW_FINISHED",
      status: "COMPLETED",
      at: 2,
    });

    expect(
      reduceWorkflowState(state, {
        type: "TASK_DISPATCHED",
        taskId: "a",
        attempt: 1,
        at: 3,
      })
    ).toBe(state);
  });
});
