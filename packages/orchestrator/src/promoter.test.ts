import {
  describe,
  it,
  expect,
  vi,
  beforeEach,
  afterEach,
  type MockInstance,
} from "vitest";
import { encodeRetryPayload, type RetryPayload } from "@cadence/core";

const redisMock = vi.hoisted(() => {
  const state = {
    calls: [] as string[],
    rangeReply: [] as string[],
  };
  const labels: Record<string, string> = {
    "cadence:tasks": "XADD:tasks",
    "cadence:tasks:dlq": "XADD:dlq",
  };
  return {
    state,
    call: vi.fn(async (cmd: string) => {
      if (cmd === "ZRANGEBYSCORE") {
        state.calls.push("ZRANGEBYSCORE");
        return state.rangeReply;
      }
      return null;
    }),
    xadd: vi.fn(async (stream: string) => {
      state.calls.push(labels[stream] ?? `XADD:${stream}`);
      return "1-0";
    }),
    zrem: vi.fn(async () => {
      state.calls.push("ZREM");
      return 1;
    }),
    quit: vi.fn(() => "OK"),
  };
});

vi.mock("ioredis", () => ({
  Redis: class {
    call = redisMock.call;
    xadd = redisMock.xadd;
    zrem = redisMock.zrem;
    quit = redisMock.quit;
  },
}));

import { RetryPromoter } from "./promoter.js";

const URL = "redis://127.0.0.1:6379";

const PAYLOAD: RetryPayload = {
  workflowId: "w1",
  taskId: "flaky",
  command: 'node -e "process.exit(3)"',
  timeoutMs: 5000,
  retries: 3,
  attempt: 2,
};

describe("RetryPromoter.promoteOnce", () => {
  let logSpy: MockInstance;
  let errorSpy: MockInstance;

  beforeEach(() => {
    redisMock.state.calls.length = 0;
    redisMock.state.rangeReply = [];
    redisMock.call.mockClear();
    redisMock.xadd.mockClear();
    redisMock.zrem.mockClear();
    logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    logSpy.mockRestore();
    errorSpy.mockRestore();
  });

  it("promotes a due member onto the task stream before removing it", async () => {
    redisMock.state.rangeReply = [encodeRetryPayload(PAYLOAD)];

    const promoter = new RetryPromoter(URL);
    const result = await promoter.promoteOnce();

    expect(result.due).toBe(1);
    expect(result.promoted).toEqual(["flaky"]);
    expect(result.dropped).toEqual([]);
    // Publish-before-remove: XADD happens before ZREM.
    expect(redisMock.state.calls).toEqual([
      "ZRANGEBYSCORE",
      "XADD:tasks",
      "ZREM",
    ]);
    // Re-dispatched as the attempt the worker recorded.
    expect(redisMock.xadd.mock.calls[0]).toContain("2");
    expect(redisMock.zrem.mock.calls[0]).toContain(encodeRetryPayload(PAYLOAD));
    await promoter.disconnect();
  });

  it("does nothing when no retry is due yet", async () => {
    redisMock.state.rangeReply = [];

    const promoter = new RetryPromoter(URL);
    const result = await promoter.promoteOnce();

    expect(result).toEqual({ due: 0, promoted: [], dropped: [] });
    expect(redisMock.state.calls).toEqual(["ZRANGEBYSCORE"]);
    expect(redisMock.xadd).not.toHaveBeenCalled();
    expect(redisMock.zrem).not.toHaveBeenCalled();
    await promoter.disconnect();
  });

  it("routes a malformed member to the DLQ and removes it, never silently", async () => {
    redisMock.state.rangeReply = ["not-json"];

    const promoter = new RetryPromoter(URL);
    const result = await promoter.promoteOnce();

    expect(result.promoted).toEqual([]);
    expect(result.dropped).toEqual(["not-json"]);
    expect(redisMock.state.calls).toEqual([
      "ZRANGEBYSCORE",
      "XADD:dlq",
      "ZREM",
    ]);
    expect(redisMock.xadd.mock.calls[0]).toContain("RETRY_MALFORMED");
    expect(errorSpy).toHaveBeenCalled();
    await promoter.disconnect();
  });

  it("keeps the member when the re-dispatch publish fails (crash-window safety)", async () => {
    redisMock.state.rangeReply = [encodeRetryPayload(PAYLOAD)];
    redisMock.xadd.mockRejectedValueOnce(new Error("redis down"));

    const promoter = new RetryPromoter(URL);

    await expect(promoter.promoteOnce()).rejects.toThrow("redis down");
    // Never ZREM: the retry stays due and is promoted on a later scan.
    expect(redisMock.zrem).not.toHaveBeenCalled();
    await promoter.disconnect();
  });

  it("accumulates promotion stats across scans", async () => {
    redisMock.state.rangeReply = [encodeRetryPayload(PAYLOAD)];

    const promoter = new RetryPromoter(URL);
    await promoter.promoteOnce();
    await promoter.promoteOnce();

    expect(promoter.getStats()).toEqual({ promoted: 2, dropped: 0 });
    await promoter.disconnect();
  });
});
