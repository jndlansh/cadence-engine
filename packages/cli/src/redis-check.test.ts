import {
  describe,
  it,
  expect,
  vi,
  beforeEach,
  afterEach,
  type MockInstance,
} from "vitest";
import { ENV_ERROR, ensureRedis } from "./redis-check.js";

describe("ensureRedis", () => {
  let errorSpy: MockInstance;

  beforeEach(() => {
    errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    errorSpy.mockRestore();
  });

  it("returns true and stays quiet when the ping succeeds", async () => {
    expect(await ensureRedis("redis://x", async () => {})).toBe(true);
    expect(errorSpy).not.toHaveBeenCalled();
  });

  it("returns false and prints an actionable message when Redis is unreachable", async () => {
    const ok = await ensureRedis("redis://127.0.0.1:6379", async () => {
      throw new Error("connect ECONNREFUSED 127.0.0.1:6379");
    });

    expect(ok).toBe(false);
    const printed = errorSpy.mock.calls.map((call) => String(call[0])).join("\n");
    expect(printed).toContain("cannot connect to Redis at redis://127.0.0.1:6379");
    expect(printed).toContain("ECONNREFUSED");
  });

  it("exposes a distinct environment exit code", () => {
    expect(ENV_ERROR).toBe(4);
  });
});
