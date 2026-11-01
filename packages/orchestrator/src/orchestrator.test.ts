import {
  describe,
  it,
  expect,
  vi,
  beforeEach,
  afterEach,
  type MockInstance,
} from "vitest";
import {
  parseWorkflowYAML,
  type ParsedWorkflow,
  type TaskCompletion,
  type TaskDefinition,
  type WorkflowStateSnapshot,
} from "@cadence/core";
import {
  WorkflowOrchestrator,
  type CompletionSource,
  type TaskSink,
} from "./orchestrator.js";
import { parseCompletionEntry } from "./completions.js";
import type { WorkflowStateStore } from "./state-store.js";
import type { WorkflowEvent, WorkflowEventSink } from "./event-logger.js";

const WF = "wf_test";

class FakeSink implements TaskSink {
  readonly waves: string[][] = [];

  async dispatchTasks(
    _workflowId: string,
    tasks: TaskDefinition[]
  ): Promise<string[]> {
    this.waves.push(tasks.map((t) => t.id));
    return tasks.map((t) => `stream-${t.id}`);
  }
}

class FakeSource implements CompletionSource {
  private readonly queue: TaskCompletion[] = [];

  push(...completions: TaskCompletion[]): void {
    this.queue.push(...completions);
  }

  async read(): Promise<TaskCompletion[] | null> {
    const next = this.queue.shift();
    return next ? [next] : null;
  }
}

class FakeStateStore implements WorkflowStateStore {
  readonly saves: WorkflowStateSnapshot[] = [];
  failNext = false;

  async save(snapshot: WorkflowStateSnapshot): Promise<void> {
    if (this.failNext) {
      this.failNext = false;
      throw new Error("state store down");
    }
    this.saves.push(snapshot);
  }

  async load(): Promise<WorkflowStateSnapshot | null> {
    return this.saves.length > 0 ? this.saves[this.saves.length - 1] : null;
  }

  async disconnect(): Promise<void> {}
}

class FakeLogger implements WorkflowEventSink {
  readonly events: WorkflowEvent[] = [];

  async emit(event: WorkflowEvent): Promise<void> {
    this.events.push(event);
  }

  async close(): Promise<void> {}
}

function completion(
  taskId: string,
  status: TaskCompletion["status"] = "COMPLETED",
  workflowId = WF
): TaskCompletion {
  return {
    workflowId,
    taskId,
    status,
    attempt: 1,
    exitCode: status === "COMPLETED" ? 0 : 1,
    durationMs: 5,
  };
}

function workflow(yamlText: string): ParsedWorkflow {
  return parseWorkflowYAML(yamlText);
}

const LINEAR = `
version: "1.0"
workflow: linear
tasks:
  - id: a
    command: echo a
  - id: b
    command: echo b
    depends_on: [a]
  - id: c
    command: echo c
    depends_on: [b]
`;

const DIAMOND = `
version: "1.0"
workflow: diamond
tasks:
  - id: extract
    command: echo extract
  - id: transform_a
    command: echo transform_a
    depends_on: [extract]
  - id: transform_b
    command: echo transform_b
    depends_on: [extract]
  - id: load
    command: echo load
    depends_on: [transform_a, transform_b]
`;

const TWO_ROOTS = `
version: "1.0"
workflow: two_roots
tasks:
  - id: root_a
    command: echo root_a
  - id: root_b
    command: echo root_b
`;

const SINGLE = `
version: "1.0"
workflow: single
tasks:
  - id: only
    command: echo only
`;

describe("WorkflowOrchestrator", () => {
  let logSpy: MockInstance;
  let errorSpy: MockInstance;
  let warnSpy: MockInstance;

  beforeEach(() => {
    logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
  });

  afterEach(() => {
    logSpy.mockRestore();
    errorSpy.mockRestore();
    warnSpy.mockRestore();
  });

  it("dispatches one task per wave for a linear chain", async () => {
    const sink = new FakeSink();
    const source = new FakeSource();
    source.push(completion("a"), completion("b"), completion("c"));

    const result = await new WorkflowOrchestrator(sink, source).run(
      WF,
      workflow(LINEAR)
    );

    expect(sink.waves).toEqual([["a"], ["b"], ["c"]]);
    expect(result.status).toBe("COMPLETED");
    expect(result.completed).toEqual(["a", "b", "c"]);
  });

  it("holds children until every parent completes, then dispatches them together", async () => {
    const sink = new FakeSink();
    const source = new FakeSource();
    source.push(
      completion("extract"),
      completion("transform_a"),
      completion("transform_b"),
      completion("load")
    );

    const result = await new WorkflowOrchestrator(sink, source).run(
      WF,
      workflow(DIAMOND)
    );

    // `load` waits for BOTH transforms; transform_b is not re-dispatched when
    // transform_a completes.
    expect(sink.waves).toEqual([
      ["extract"],
      ["transform_a", "transform_b"],
      ["load"],
    ]);
    expect(result.status).toBe("COMPLETED");
  });

  it("does not re-dispatch dependency-free roots on later completions", async () => {
    const sink = new FakeSink();
    const source = new FakeSource();
    source.push(completion("root_a"), completion("root_b"));

    const result = await new WorkflowOrchestrator(sink, source).run(
      WF,
      workflow(TWO_ROOTS)
    );

    expect(sink.waves).toEqual([["root_a", "root_b"]]);
    expect(result.status).toBe("COMPLETED");
  });

  it("treats a duplicated completion as idempotent", async () => {
    const sink = new FakeSink();
    const source = new FakeSource();
    source.push(
      completion("a"),
      completion("a"),
      completion("b"),
      completion("c")
    );

    const result = await new WorkflowOrchestrator(sink, source).run(
      WF,
      workflow(LINEAR)
    );

    expect(sink.waves).toEqual([["a"], ["b"], ["c"]]);
    expect(result.status).toBe("COMPLETED");
  });

  it("ignores completions for tasks it never dispatched", async () => {
    const sink = new FakeSink();
    const source = new FakeSource();
    source.push(
      completion("ghost"),
      completion("a"),
      completion("b"),
      completion("c")
    );

    const result = await new WorkflowOrchestrator(sink, source).run(
      WF,
      workflow(LINEAR)
    );

    expect(sink.waves).toEqual([["a"], ["b"], ["c"]]);
    expect(result.status).toBe("COMPLETED");
  });

  it("ignores completions belonging to another workflow", async () => {
    const sink = new FakeSink();
    const source = new FakeSource();
    source.push(
      completion("a", "COMPLETED", "some_other_workflow"),
      completion("a"),
      completion("b"),
      completion("c")
    );

    const result = await new WorkflowOrchestrator(sink, source).run(
      WF,
      workflow(LINEAR)
    );

    expect(sink.waves).toEqual([["a"], ["b"], ["c"]]);
    expect(result.status).toBe("COMPLETED");
  });

  it("aborts without dispatching descendants when a task fails terminally", async () => {
    const sink = new FakeSink();
    const source = new FakeSource();
    source.push(completion("a", "FAILED"));

    const result = await new WorkflowOrchestrator(sink, source).run(
      WF,
      workflow(LINEAR)
    );

    expect(sink.waves).toEqual([["a"]]);
    expect(result.status).toBe("FAILED");
    expect(result.failed).toEqual(["a"]);
    expect(result.completed).toEqual([]);
  });

  it("drains in-flight siblings before reporting FAILED", async () => {
    const sink = new FakeSink();
    const source = new FakeSource();
    source.push(completion("root_a", "FAILED"), completion("root_b"));

    const result = await new WorkflowOrchestrator(sink, source).run(
      WF,
      workflow(TWO_ROOTS)
    );

    expect(sink.waves).toEqual([["root_a", "root_b"]]);
    expect(result.status).toBe("FAILED");
    expect(result.failed).toEqual(["root_a"]);
    expect(result.completed).toEqual(["root_b"]);
  });

  it("reports STALLED when no completion ever arrives", async () => {
    const sink = new FakeSink();
    const source = new FakeSource();

    const result = await new WorkflowOrchestrator(sink, source, {
      completionTimeoutMs: 0,
    }).run(WF, workflow(LINEAR));

    expect(result.status).toBe("STALLED");
    expect(sink.waves).toEqual([["a"]]);
    expect(result.completed).toEqual([]);
  });

  it("persists lifecycle transitions and per-task records to the state store", async () => {
    const sink = new FakeSink();
    const source = new FakeSource();
    source.push(completion("a"), completion("b"), completion("c"));
    const store = new FakeStateStore();

    const result = await new WorkflowOrchestrator(sink, source, {
      stateStore: store,
    }).run(WF, workflow(LINEAR));

    expect(result.status).toBe("COMPLETED");
    expect(result.state.state).toBe("COMPLETED");
    // First persist is the RUNNING transition, last is the terminal one.
    expect(store.saves[0].state).toBe("RUNNING");
    expect(store.saves[store.saves.length - 1].state).toBe("COMPLETED");
    expect(store.saves[store.saves.length - 1].tasks["c"]).toMatchObject({
      status: "COMPLETED",
      attempt: 1,
    });
  });

  it("persists a terminal FAILED state with the failed task", async () => {
    const sink = new FakeSink();
    const source = new FakeSource();
    source.push(completion("a", "FAILED"));
    const store = new FakeStateStore();

    const result = await new WorkflowOrchestrator(sink, source, {
      stateStore: store,
    }).run(WF, workflow(LINEAR));

    expect(result.status).toBe("FAILED");
    const final = store.saves[store.saves.length - 1];
    expect(final.state).toBe("FAILED");
    expect(final.failed).toEqual(["a"]);
    expect(final.tasks["a"].status).toBe("FAILED");
    expect(final.completedAt).toBeDefined();
  });

  it("persists STALLED when no completion ever arrives", async () => {
    const store = new FakeStateStore();

    const result = await new WorkflowOrchestrator(new FakeSink(), new FakeSource(), {
      completionTimeoutMs: 0,
      stateStore: store,
    }).run(WF, workflow(LINEAR));

    expect(result.status).toBe("STALLED");
    expect(store.saves[store.saves.length - 1].state).toBe("STALLED");
  });

  it("keeps orchestrating when the state store fails to persist", async () => {
    const sink = new FakeSink();
    const source = new FakeSource();
    source.push(completion("a"), completion("b"), completion("c"));
    const store = new FakeStateStore();
    store.failNext = true; // the WORKFLOW_STARTED save fails

    const result = await new WorkflowOrchestrator(sink, source, {
      stateStore: store,
    }).run(WF, workflow(LINEAR));

    expect(result.status).toBe("COMPLETED");
    expect(errorSpy).toHaveBeenCalled();
  });

  it("emits structured lifecycle events to the logger", async () => {
    const sink = new FakeSink();
    const source = new FakeSource();
    source.push(completion("only"));
    const logger = new FakeLogger();

    const result = await new WorkflowOrchestrator(sink, source, {
      logger,
    }).run(WF, workflow(SINGLE));

    const types = logger.events.map((event) => event.type);
    expect(types).toContain("WORKFLOW_STARTED");
    expect(types).toContain("TASK_DISPATCHED");
    expect(types).toContain("TASK_COMPLETED");
    expect(types[types.length - 1]).toBe("WORKFLOW_FINISHED");
    expect(result.status).toBe("COMPLETED");
  });
});

describe("parseCompletionEntry", () => {
  let errorSpy: MockInstance;

  beforeEach(() => {
    errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    errorSpy.mockRestore();
  });

  it("folds flat stream fields and coerces numeric strings", () => {
    expect(
      parseCompletionEntry([
        "workflowId",
        "w1",
        "taskId",
        "t1",
        "status",
        "COMPLETED",
        "attempt",
        "2",
        "exitCode",
        "0",
        "durationMs",
        "12",
      ])
    ).toEqual({
      workflowId: "w1",
      taskId: "t1",
      status: "COMPLETED",
      attempt: 2,
      exitCode: 0,
      durationMs: 12,
    });
  });

  it("returns null and logs for a malformed entry", () => {
    expect(parseCompletionEntry(["workflowId", "w1"])).toBeNull();
    expect(errorSpy).toHaveBeenCalled();
  });

  it("returns null for an unknown status", () => {
    expect(
      parseCompletionEntry([
        "workflowId",
        "w1",
        "taskId",
        "t1",
        "status",
        "MAYBE",
        "attempt",
        "1",
        "exitCode",
        "0",
        "durationMs",
        "1",
      ])
    ).toBeNull();
  });

  it("decodes timedOut=false as false, not by string truthiness", () => {
    const parsed = parseCompletionEntry([
      "workflowId",
      "w1",
      "taskId",
      "t1",
      "status",
      "FAILED",
      "attempt",
      "1",
      "exitCode",
      "1",
      "durationMs",
      "5",
      "timedOut",
      "false",
      "error",
      "boom",
    ]);

    expect(parsed).toMatchObject({ timedOut: false, error: "boom" });
  });

  it("decodes timedOut=true as true", () => {
    const parsed = parseCompletionEntry([
      "workflowId",
      "w1",
      "taskId",
      "t1",
      "status",
      "FAILED",
      "attempt",
      "1",
      "exitCode",
      "124",
      "durationMs",
      "1000",
      "timedOut",
      "true",
    ]);

    expect(parsed?.timedOut).toBe(true);
  });
});
