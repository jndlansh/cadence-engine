import {
  describe,
  it,
  expect,
  vi,
  beforeEach,
  afterEach,
  type MockInstance,
} from "vitest";
import type { WorkflowStateSnapshot } from "@cadence/core";

const redisMock = vi.hoisted(() => {
  const state = {
    hsetCalls: [] as string[][],
    hash: {} as Record<string, string>,
  };
  return {
    state,
    hset: vi.fn(async (key: string, ...fields: string[]) => {
      state.hsetCalls.push([key, ...fields]);
      return 1;
    }),
    hgetall: vi.fn(async () => state.hash),
    quit: vi.fn(() => "OK"),
  };
});

vi.mock("ioredis", () => ({
  Redis: class {
    hset = redisMock.hset;
    hgetall = redisMock.hgetall;
    quit = redisMock.quit;
  },
}));

import { RedisWorkflowStateStore } from "./state-store.js";

const URL = "redis://127.0.0.1:6379";
const KEY = "workflow:wf1:state";

const SNAPSHOT: WorkflowStateSnapshot = {
  workflowId: "wf1",
  state: "RUNNING",
  startedAt: 100,
  updatedAt: 200,
  failed: [],
  tasks: {
    a: {
      taskId: "a",
      status: "COMPLETED",
      attempt: 1,
      dispatchedAt: 110,
      completedAt: 190,
      durationMs: 40,
      latencyMs: 80,
      exitCode: 0,
    },
  },
};

/** Field/value pairs from a recorded HSET call, as a lookup record. */
function savedFields(): Record<string, string> {
  const [, ...flat] = redisMock.state.hsetCalls[0];
  const record: Record<string, string> = {};
  for (let i = 0; i + 1 < flat.length; i += 2) {
    record[flat[i]] = flat[i + 1];
  }
  return record;
}

describe("RedisWorkflowStateStore", () => {
  let errorSpy: MockInstance;

  beforeEach(() => {
    redisMock.state.hsetCalls.length = 0;
    redisMock.state.hash = {};
    redisMock.hset.mockClear();
    redisMock.hgetall.mockClear();
    errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    errorSpy.mockRestore();
  });

  it("saves the snapshot as a hash under the workflow state key", async () => {
    const store = new RedisWorkflowStateStore(URL);
    await store.save(SNAPSHOT);

    expect(redisMock.state.hsetCalls[0][0]).toBe(KEY);
    const fields = savedFields();
    expect(fields["workflowId"]).toBe("wf1");
    expect(fields["state"]).toBe("RUNNING");
    expect(fields["startedAt"]).toBe("100");
    expect(JSON.parse(fields["failed"])).toEqual([]);
    expect(JSON.parse(fields["tasks"])).toEqual(SNAPSHOT.tasks);
    await store.disconnect();
  });

  it("writes an empty completedAt while the run is still active", async () => {
    const store = new RedisWorkflowStateStore(URL);
    await store.save(SNAPSHOT);

    expect(savedFields()["completedAt"]).toBe("");
    await store.disconnect();
  });

  it("loads and coerces a persisted snapshot", async () => {
    redisMock.state.hash = {
      workflowId: "wf1",
      state: "COMPLETED",
      startedAt: "100",
      updatedAt: "300",
      completedAt: "300",
      failed: JSON.stringify([]),
      tasks: JSON.stringify(SNAPSHOT.tasks),
    };

    const store = new RedisWorkflowStateStore(URL);
    const loaded = await store.load("wf1");

    expect(loaded).toEqual({
      workflowId: "wf1",
      state: "COMPLETED",
      startedAt: 100,
      updatedAt: 300,
      completedAt: 300,
      failed: [],
      tasks: SNAPSHOT.tasks,
    });
    await store.disconnect();
  });

  it("returns null for a workflow that was never persisted", async () => {
    const store = new RedisWorkflowStateStore(URL);
    expect(await store.load("missing")).toBeNull();
    await store.disconnect();
  });

  it("returns null and logs when the persisted blob is not JSON", async () => {
    redisMock.state.hash = {
      workflowId: "wf1",
      state: "RUNNING",
      startedAt: "1",
      updatedAt: "2",
      failed: "not-json",
      tasks: "{}",
    };

    const store = new RedisWorkflowStateStore(URL);
    expect(await store.load("wf1")).toBeNull();
    expect(errorSpy).toHaveBeenCalled();
    await store.disconnect();
  });

  it("returns null and logs when the persisted state fails validation", async () => {
    redisMock.state.hash = {
      workflowId: "wf1",
      state: "BOGUS",
      startedAt: "1",
      updatedAt: "2",
      failed: "[]",
      tasks: "{}",
    };

    const store = new RedisWorkflowStateStore(URL);
    expect(await store.load("wf1")).toBeNull();
    expect(errorSpy).toHaveBeenCalled();
    await store.disconnect();
  });
});
