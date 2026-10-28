import { describe, it, expect } from "vitest";
import { foldStreamFields, heartbeatKey } from "./streams.js";

describe("foldStreamFields", () => {
  it("folds a flat field/value array into a lookup record", () => {
    expect(
      foldStreamFields(["workflowId", "w1", "taskId", "t1", "attempt", "1"])
    ).toEqual({ workflowId: "w1", taskId: "t1", attempt: "1" });
  });

  it("returns an empty record for an empty array", () => {
    expect(foldStreamFields([])).toEqual({});
  });

  it("ignores a trailing field that has no value", () => {
    expect(foldStreamFields(["taskId", "t1", "dangling"])).toEqual({
      taskId: "t1",
    });
  });

  it("keeps the last occurrence of a duplicated field", () => {
    expect(foldStreamFields(["a", "1", "a", "2"])).toEqual({ a: "2" });
  });
});

describe("heartbeatKey", () => {
  it("namespaces the key under the worker id", () => {
    expect(heartbeatKey("worker_123_456")).toBe(
      "worker:worker_123_456:heartbeat"
    );
  });
});
