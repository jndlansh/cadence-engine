import {
  describe,
  it,
  expect,
  vi,
  beforeEach,
  afterEach,
  type MockInstance,
} from "vitest";
import { WorkerConsumer, fieldsToRecord } from "./consumer.js";

const TASKS_STREAM = "cadence:tasks";
const DLQ_STREAM = "cadence:tasks:dlq";
const COMPLETION_STREAM = "cadence:completions";

const redisMock = vi.hoisted(() => {
  const calls: string[] = [];
  const published: string[] = [];
  const labels: Record<string, string> = {
    "cadence:tasks": "XADD:tasks",
    "cadence:tasks:dlq": "XADD:dlq",
    "cadence:completions": "XADD:completions",
  };
  return {
    calls,
    published,
    xadd: vi.fn(async (stream: string) => {
      calls.push(labels[stream] ?? `XADD:${stream}`);
      published.push(stream);
      return "1-0";
    }),
    xack: vi.fn(async () => {
      calls.push("XACK");
      return 1;
    }),
    xgroup: vi.fn(async () => "OK"),
    call: vi.fn(async () => null),
    quit: vi.fn(() => "OK"),
  };
});

vi.mock("ioredis", () => ({
  Redis: class {
    xadd = redisMock.xadd;
    xack = redisMock.xack;
    xgroup = redisMock.xgroup;
    call = redisMock.call;
    quit = redisMock.quit;
  },
}));

type ConsumerInternals = {
  handleMessage(messageId: string, fields: Record<string, string>): Promise<void>;
};

function internals(consumer: WorkerConsumer): ConsumerInternals {
  return consumer as unknown as ConsumerInternals;
}

describe("fieldsToRecord", () => {
  it("folds a flat field/value array into a lookup record", () => {
    expect(
      fieldsToRecord([
        "workflowId",
        "w1",
        "taskId",
        "t1",
        "command",
        "echo hi",
        "timeoutMs",
        "5000",
        "retries",
        "0",
        "attempt",
        "1",
      ])
    ).toEqual({
      workflowId: "w1",
      taskId: "t1",
      command: "echo hi",
      timeoutMs: "5000",
      retries: "0",
      attempt: "1",
    });
  });

  it("returns an empty record for an empty field array", () => {
    expect(fieldsToRecord([])).toEqual({});
  });

  it("ignores a trailing field that has no value", () => {
    expect(fieldsToRecord(["taskId", "t1", "dangling"])).toEqual({
      taskId: "t1",
    });
  });

  it("keeps the last occurrence of a duplicated field", () => {
    expect(fieldsToRecord(["a", "1", "a", "2"])).toEqual({ a: "2" });
  });
});

describe("WorkerConsumer.handleMessage", () => {
  let logSpy: MockInstance;

  beforeEach(() => {
    redisMock.calls.length = 0;
    redisMock.published.length = 0;
    redisMock.xadd.mockClear();
    redisMock.xack.mockClear();
    logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
  });

  afterEach(() => {
    logSpy.mockRestore();
  });

  it("publishes a completion BEFORE acknowledging a successful task", async () => {
    const consumer = new WorkerConsumer("redis://127.0.0.1:6379");

    await internals(consumer).handleMessage("1-0", {
      workflowId: "w1",
      taskId: "ok",
      command: "echo done",
      timeoutMs: "5000",
      retries: "0",
      attempt: "1",
    });

    expect(redisMock.calls).toEqual(["XADD:completions", "XACK"]);
    expect(redisMock.published).toEqual([COMPLETION_STREAM]);
    expect(redisMock.xadd.mock.calls[0]).toContain("COMPLETED");
  });

  it("re-publishes the retry BEFORE acknowledging and emits no completion", async () => {
    const consumer = new WorkerConsumer("redis://127.0.0.1:6379");

    await internals(consumer).handleMessage("2-0", {
      workflowId: "w1",
      taskId: "flaky",
      command: 'node -e "process.exit(3)"',
      timeoutMs: "5000",
      retries: "3",
      attempt: "1",
    });

    expect(redisMock.calls).toEqual(["XADD:tasks", "XACK"]);
    expect(redisMock.published).toEqual([TASKS_STREAM]);
  });

  it("routes to the DLQ, then the completion, then acks once retries are exhausted", async () => {
    const consumer = new WorkerConsumer("redis://127.0.0.1:6379");

    await internals(consumer).handleMessage("3-0", {
      workflowId: "w1",
      taskId: "doomed",
      command: 'node -e "process.exit(7)"',
      timeoutMs: "5000",
      retries: "1",
      attempt: "1",
    });

    expect(redisMock.calls).toEqual(["XADD:dlq", "XADD:completions", "XACK"]);
    expect(redisMock.published).toEqual([DLQ_STREAM, COMPLETION_STREAM]);
    expect(redisMock.xadd.mock.calls[1]).toContain("FAILED");
  });

  it("never acknowledges when the follow-up publish fails (crash-window safety)", async () => {
    const consumer = new WorkerConsumer("redis://127.0.0.1:6379");
    redisMock.xadd.mockRejectedValueOnce(new Error("redis down"));

    await expect(
      internals(consumer).handleMessage("4-0", {
        workflowId: "w1",
        taskId: "flaky",
        command: 'node -e "process.exit(3)"',
        timeoutMs: "5000",
        retries: "3",
        attempt: "1",
      })
    ).rejects.toThrow("redis down");

    // The entry must remain pending in the PEL so it stays recoverable.
    expect(redisMock.xack).not.toHaveBeenCalled();
  });

  it("quarantines a malformed entry to the DLQ without emitting a completion", async () => {
    const consumer = new WorkerConsumer("redis://127.0.0.1:6379");
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    await internals(consumer).handleMessage("5-0", { workflowId: "w1" });

    expect(redisMock.calls).toEqual(["XADD:dlq", "XACK"]);
    expect(redisMock.published).toEqual([DLQ_STREAM]);
    expect(redisMock.xadd.mock.calls[0]).toContain("MALFORMED_ENTRY");
    expect(errorSpy).toHaveBeenCalled();

    errorSpy.mockRestore();
  });
});
