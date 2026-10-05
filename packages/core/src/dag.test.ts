import { describe, it, expect } from "vitest";
import { DAGResolver } from "./dag.js";
import { TaskDefinition } from "./types.js";

describe("DAGResolver", () => {
  it("should identify root tasks with zero dependencies", () => {
    const tasks: TaskDefinition[] = [
      { id: "A", command: "echo A", depends_on: [], retries: 0, timeout_ms: 5000 },
      { id: "B", command: "echo B", depends_on: ["A"], retries: 0, timeout_ms: 5000 },
      { id: "C", command: "echo C", depends_on: [], retries: 0, timeout_ms: 5000 },
    ];

    const dag = new DAGResolver(tasks);
    const initial = dag.getInitialTasks().map((t) => t.id);

    expect(initial).toEqual(expect.arrayContaining(["A", "C"]));
    expect(initial).not.toContain("B");
  });

  it("should unlock dependent tasks once parents finish", () => {
    const tasks: TaskDefinition[] = [
      { id: "A", command: "echo A", depends_on: [], retries: 0, timeout_ms: 5000 },
      { id: "B", command: "echo B", depends_on: ["A"], retries: 0, timeout_ms: 5000 },
      { id: "C", command: "echo C", depends_on: ["A", "B"], retries: 0, timeout_ms: 5000 },
    ];

    const dag = new DAGResolver(tasks);

    // Only A completed -> B should unlock, but not C
    const step1 = dag.getNextExecutableTasks(new Set(["A"])).map((t) => t.id);
    expect(step1).toEqual(["B"]);

    // Both A and B completed -> C unlocks
    const step2 = dag.getNextExecutableTasks(new Set(["A", "B"])).map((t) => t.id);
    expect(step2).toEqual(["C"]);
  });

  it("should throw an error if a cycle is detected", () => {
    const cyclicTasks: TaskDefinition[] = [
      { id: "A", command: "echo A", depends_on: ["B"], retries: 0, timeout_ms: 5000 },
      { id: "B", command: "echo B", depends_on: ["A"], retries: 0, timeout_ms: 5000 },
    ];

    expect(() => new DAGResolver(cyclicTasks)).toThrow(
      "Invalid Workflow: Cycle detected in task dependency graph."
    );
  });

  it("should throw an error if a task depends on a non-existent task", () => {
    const orphanTasks: TaskDefinition[] = [
      { id: "A", command: "echo A", depends_on: ["Ghost"], retries: 0, timeout_ms: 5000 },
    ];

    expect(() => new DAGResolver(orphanTasks)).toThrow(
      'Task "A" depends on undefined task "Ghost".'
    );
  });
});