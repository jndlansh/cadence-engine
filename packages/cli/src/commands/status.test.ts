import { describe, it, expect } from "vitest";
import type { WorkflowStateSnapshot } from "@cadence/core";
import { formatStatus } from "./status.js";

const SNAPSHOT: WorkflowStateSnapshot = {
  workflowId: "wf1",
  state: "FAILED",
  startedAt: Date.UTC(2026, 0, 1),
  updatedAt: Date.UTC(2026, 0, 1, 0, 0, 5),
  completedAt: Date.UTC(2026, 0, 1, 0, 0, 5),
  cursor: "3-0",
  failed: ["aggregate"],
  tasks: {
    extract: {
      taskId: "extract",
      status: "COMPLETED",
      attempt: 1,
      dispatchedAt: 0,
      latencyMs: 12,
      exitCode: 0,
    },
    aggregate: {
      taskId: "aggregate",
      status: "FAILED",
      attempt: 2,
      dispatchedAt: 0,
      latencyMs: 30,
      exitCode: 1,
      error: "boom",
    },
  },
};

describe("formatStatus", () => {
  it("includes the workflow id, state and timestamps", () => {
    const out = formatStatus(SNAPSHOT);

    expect(out).toContain("workflow : wf1");
    expect(out).toContain("state    : FAILED");
    expect(out).toContain("completed:");
    expect(out).toContain("failed   : aggregate");
  });

  it("renders one sorted row per task with detail", () => {
    const taskLines = formatStatus(SNAPSHOT)
      .split("\n")
      .filter((line) => line.startsWith("  "));

    expect(taskLines).toHaveLength(2);
    expect(taskLines[0]).toContain("aggregate");
    expect(taskLines[0]).toContain("attempt 2");
    expect(taskLines[0]).toContain("error: boom");
    expect(taskLines[1]).toContain("extract");
  });

  it("shows '-' when nothing failed", () => {
    expect(
      formatStatus({ ...SNAPSHOT, failed: [] })
    ).toContain("failed   : -");
  });
});
