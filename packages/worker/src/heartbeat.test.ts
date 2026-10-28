import {
  describe,
  it,
  expect,
  vi,
  beforeEach,
  afterEach,
  type MockInstance,
} from "vitest";

const redisMock = vi.hoisted(() => ({
  set: vi.fn(async () => "OK"),
  del: vi.fn(async () => 1),
  quit: vi.fn(() => "OK"),
}));

vi.mock("ioredis", () => ({
  Redis: class {
    set = redisMock.set;
    del = redisMock.del;
    quit = redisMock.quit;
  },
}));

// Imported after the mock is registered.
import { WorkerHeartbeat } from "./heartbeat.js";

const URL = "redis://127.0.0.1:6379";
const KEY = "worker:worker_1:heartbeat";

describe("WorkerHeartbeat", () => {
  beforeEach(() => {
    redisMock.set.mockClear();
    redisMock.del.mockClear();
    redisMock.quit.mockClear();
    redisMock.set.mockResolvedValue("OK");
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("writes the heartbeat key with the TTL immediately on start", async () => {
    const hb = new WorkerHeartbeat("worker_1", URL, {
      intervalMs: 1000,
      ttlSeconds: 10,
    });

    await hb.start();

    expect(redisMock.set).toHaveBeenCalledTimes(1);
    expect(redisMock.set).toHaveBeenCalledWith(KEY, "alive", "EX", 10);
    await hb.disconnect();
  });

  it("renews the heartbeat on the configured interval", async () => {
    const hb = new WorkerHeartbeat("worker_1", URL, {
      intervalMs: 1000,
      ttlSeconds: 10,
    });

    await hb.start();
    await vi.advanceTimersByTimeAsync(1000);
    expect(redisMock.set).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1000);
    expect(redisMock.set).toHaveBeenCalledTimes(3);
    await hb.disconnect();
  });

  it("stops renewing (and closes the connection) after disconnect", async () => {
    const hb = new WorkerHeartbeat("worker_1", URL, {
      intervalMs: 1000,
      ttlSeconds: 10,
    });

    await hb.start();
    await hb.disconnect();
    const callsAtDisconnect = redisMock.set.mock.calls.length;

    await vi.advanceTimersByTimeAsync(5000);

    expect(redisMock.set.mock.calls.length).toBe(callsAtDisconnect);
    // Graceful stop drops the key so the reclaimer can act immediately.
    expect(redisMock.del).toHaveBeenCalledWith(KEY);
    expect(redisMock.quit).toHaveBeenCalledTimes(1);
  });

  it("logs and survives a failed beat rather than throwing from the timer", async () => {
    const errorSpy: MockInstance = vi
      .spyOn(console, "error")
      .mockImplementation(() => {});

    const hb = new WorkerHeartbeat("worker_1", URL, {
      intervalMs: 1000,
      ttlSeconds: 10,
    });
    await hb.start();

    redisMock.set.mockRejectedValueOnce(new Error("redis down"));
    await vi.advanceTimersByTimeAsync(1000);

    expect(errorSpy).toHaveBeenCalled();
    errorSpy.mockRestore();
    await hb.disconnect();
  });
});
