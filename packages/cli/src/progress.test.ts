import { describe, it, expect } from "vitest";
import type { TaskRunRecord, WorkflowStateSnapshot } from "@cadence/core";
import { buildProgressView } from "./progress.js";

function record(
  taskId: string,
  status: TaskRunRecord["status"]
): TaskRunRecord {
  return { taskId, status, attempt: 1, dispatchedAt: 10 };
}

function snapshot(records: TaskRunRecord[]): WorkflowStateSnapshot {
  const tasks: Record<string, TaskRunRecord> = {};
  for (const r of records) tasks[r.taskId] = r;

  return {
    workflowId: "wf",
    state: "RUNNING",
    startedAt: 0,
    updatedAt: 0,
    cursor: "0",
    failed: [],
    tasks,
  };
}

describe("buildProgressView", () => {
  it("counts a fresh snapshot as all pending", () => {
    const view = buildProgressView(
      snapshot([record("a", "PENDING"), record("b", "PENDING")])
    );

    expect(view).toMatchObject({
      total: 2,
      pending: 2,
      running: 0,
      completed: 0,
      failed: 0,
      percent: 0,
    });
    expect(view.label).toBe("0/2 done · 2 pending");
  });

  it("counts running, completed and failed tasks", () => {
    const view = buildProgressView(
      snapshot([
        record("a", "COMPLETED"),
        record("b", "RUNNING"),
        record("c", "FAILED"),
      ])
    );

    expect(view).toMatchObject({
      total: 3,
      pending: 0,
      running: 1,
      completed: 1,
      failed: 1,
    });
    expect(view.percent).toBeCloseTo(2 / 3);
    expect(view.label).toBe("2/3 done · 1 running · 1 failed");
  });

  it("reports 100% when every task has finished", () => {
    const view = buildProgressView(
      snapshot([record("a", "COMPLETED"), record("b", "COMPLETED")])
    );

    expect(view.percent).toBe(1);
    expect(view.label).toBe("2/2 done");
  });

  it("treats an empty snapshot as zero without dividing by zero", () => {
    const view = buildProgressView(snapshot([]));

    expect(view.total).toBe(0);
    expect(view.percent).toBe(0);
    expect(Number.isNaN(view.percent)).toBe(false);
  });
});
