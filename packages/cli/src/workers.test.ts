import { describe, it, expect } from "vitest";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { parseWorkerReady, resolveRepoRoot } from "./workers.js";

describe("parseWorkerReady", () => {
  it("extracts the worker id from the ready line", () => {
    expect(parseWorkerReady("WORKER_READY id=worker_123_456")).toBe(
      "worker_123_456"
    );
  });

  it("returns null for unrelated worker output", () => {
    expect(parseWorkerReady("[worker] running task foo")).toBeNull();
  });

  it("finds the id inside a larger buffered chunk", () => {
    expect(parseWorkerReady("noise\nWORKER_READY id=abc\ntrailing")).toBe("abc");
  });
});

describe("resolveRepoRoot", () => {
  it("resolves to the repo root that holds scripts/worker-node.ts", () => {
    const root = resolveRepoRoot();
    expect(existsSync(join(root, "scripts", "worker-node.ts"))).toBe(true);
    expect(existsSync(join(root, "pnpm-workspace.yaml"))).toBe(true);
  });
});
