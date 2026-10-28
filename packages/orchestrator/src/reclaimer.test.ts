import {
  describe,
  it,
  expect,
  vi,
  beforeEach,
  type MockInstance,
} from "vitest";

type PendingEntry = [string, string, number, number];
type ClaimReply = null | Array<[string, string[]]>;

const redisMock = vi.hoisted(() => {
  const state = {
    calls: [] as string[],
    pending: [] as PendingEntry[],
    claimReply: null as ClaimReply,
    existsResult: 0,
  };
  const labels: Record<string, string> = {
    "cadence:tasks": "XADD:tasks",
    "cadence:tasks:dlq": "XADD:dlq",
    "cadence:completions": "XADD:completions",
  };
  return {
    state,
    exists: vi.fn(async () => state.existsResult),
    xadd: vi.fn(async (stream: string) => {
      state.calls.push(labels[stream] ?? `XADD:${stream}`);
      return "1-0";
    }),
    xack: vi.fn(async () => {
      state.calls.push("XACK");
      return 1;
    }),
    call: vi.fn(async (cmd: string) => {
      if (cmd === "XPENDING") {
        state.calls.push("XPENDING");
        return state.pending;
      }
      if (cmd === "XCLAIM") {
        state.calls.push("XCLAIM");
        return state.claimReply;
      }
      return null;
    }),
    quit: vi.fn(() => "OK"),
  };
});

vi.mock("ioredis", () => ({
  Redis: class {
    exists = redisMock.exists;
    xadd = redisMock.xadd;
    xack = redisMock.xack;
    call = redisMock.call;
    quit = redisMock.quit;
  },
}));

import { TaskReclaimer } from "./reclaimer.js";

const URL = "redis://127.0.0.1:6379";

function taskFields(overrides: Record<string, string> = {}): string[] {
  const fields: Record<string, string> = {
    workflowId: "w1",
    taskId: "t1",
    command: "echo hi",
    timeoutMs: "5000",
    retries: "3",
    attempt: "1",
    ...overrides,
  };
  return Object.entries(fields).flat();
}

describe("TaskReclaimer.sweepOnce", () => {
  let warnSpy: MockInstance;
  let errorSpy: MockInstance;

  beforeEach(() => {
    redisMock.state.calls.length = 0;
    redisMock.state.pending = [];
    redisMock.state.claimReply = null;
    redisMock.state.existsResult = 0;
    redisMock.exists.mockClear();
    redisMock.xadd.mockClear();
    redisMock.xack.mockClear();
    redisMock.call.mockClear();
    warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  });

  function restore(): void {
    warnSpy.mockRestore();
    errorSpy.mockRestore();
  }

  it("reclaims a task from a dead worker, re-publishes it, then acks", async () => {
    redisMock.state.pending = [["1-0", "worker_dead", 6000, 2]];
    redisMock.state.existsResult = 0;
    redisMock.state.claimReply = [["1-0", taskFields()]];

    const reclaimer = new TaskReclaimer(URL, { minIdleMs: 5000 });
    const result = await reclaimer.sweepOnce();

    expect(result.examined).toBe(1);
    expect(result.reclaimed).toEqual(["1-0"]);
    expect(result.deadLettered).toEqual([]);
    // Publish-before-ack: the fresh entry must exist before the original is acked.
    expect(redisMock.state.calls).toEqual([
      "XPENDING",
      "XCLAIM",
      "XADD:tasks",
      "XACK",
    ]);
    // Re-dispatched as the next attempt.
    expect(redisMock.xadd.mock.calls[0]).toContain("2");
    await reclaimer.disconnect();
    restore();
  });

  it("leaves a task alone while its worker's heartbeat is still alive", async () => {
    redisMock.state.pending = [["1-0", "worker_live", 6000, 1]];
    redisMock.state.existsResult = 1;

    const reclaimer = new TaskReclaimer(URL, { minIdleMs: 5000 });
    const result = await reclaimer.sweepOnce();

    expect(result.reclaimed).toEqual([]);
    expect(redisMock.state.calls).toEqual(["XPENDING"]);
    expect(redisMock.xack).not.toHaveBeenCalled();
    await reclaimer.disconnect();
    restore();
  });

  it("ignores an entry younger than the idle margin even if its owner looks dead", async () => {
    redisMock.state.pending = [["1-0", "worker_dead", 500, 1]];
    redisMock.state.existsResult = 0;

    const reclaimer = new TaskReclaimer(URL, { minIdleMs: 5000 });
    const result = await reclaimer.sweepOnce();

    expect(result.reclaimed).toEqual([]);
    expect(redisMock.state.calls).toEqual(["XPENDING"]);
    // Never even consulted liveness for a too-fresh entry.
    expect(redisMock.exists).not.toHaveBeenCalled();
    await reclaimer.disconnect();
    restore();
  });

  it("routes to DLQ and emits a terminal FAILED completion when retries are exhausted", async () => {
    redisMock.state.pending = [["1-0", "worker_dead", 6000, 5]];
    redisMock.state.existsResult = 0;
    redisMock.state.claimReply = [
      ["1-0", taskFields({ attempt: "3", retries: "3" })],
    ];

    const reclaimer = new TaskReclaimer(URL, { minIdleMs: 5000 });
    const result = await reclaimer.sweepOnce();

    expect(result.deadLettered).toEqual(["1-0"]);
    expect(result.reclaimed).toEqual([]);
    expect(redisMock.state.calls).toEqual([
      "XPENDING",
      "XCLAIM",
      "XADD:dlq",
      "XADD:completions",
      "XACK",
    ]);
    expect(redisMock.xadd.mock.calls[1]).toContain("FAILED");
    await reclaimer.disconnect();
    restore();
  });

  it("never acks when the re-dispatch publish fails (crash-window safety)", async () => {
    redisMock.state.pending = [["1-0", "worker_dead", 6000, 2]];
    redisMock.state.existsResult = 0;
    redisMock.state.claimReply = [["1-0", taskFields()]];
    redisMock.xadd.mockRejectedValueOnce(new Error("redis down"));

    const reclaimer = new TaskReclaimer(URL, { minIdleMs: 5000 });

    await expect(reclaimer.sweepOnce()).rejects.toThrow("redis down");
    // The entry stays pending so a later sweep (or a live reclaimer) can retry.
    expect(redisMock.xack).not.toHaveBeenCalled();
    await reclaimer.disconnect();
    restore();
  });

  it("quarantines a malformed reclaimed entry to the DLQ without a completion", async () => {
    redisMock.state.pending = [["1-0", "worker_dead", 6000, 2]];
    redisMock.state.existsResult = 0;
    redisMock.state.claimReply = [
      ["1-0", ["workflowId", "w1", "command", "echo hi"]],
    ];

    const reclaimer = new TaskReclaimer(URL, { minIdleMs: 5000 });
    const result = await reclaimer.sweepOnce();

    expect(result.deadLettered).toEqual(["1-0"]);
    expect(redisMock.state.calls).toEqual([
      "XPENDING",
      "XCLAIM",
      "XADD:dlq",
      "XACK",
    ]);
    expect(redisMock.xadd.mock.calls[0]).toContain("MALFORMED_ENTRY");
    await reclaimer.disconnect();
    restore();
  });

  it("does nothing when the PEL is empty", async () => {
    const reclaimer = new TaskReclaimer(URL, { minIdleMs: 5000 });
    const result = await reclaimer.sweepOnce();

    expect(result).toEqual({ examined: 0, reclaimed: [], deadLettered: [] });
    expect(redisMock.state.calls).toEqual(["XPENDING"]);
    await reclaimer.disconnect();
    restore();
  });
});
