import { describe, it, expect } from "vitest";
import { fieldsToRecord } from "./consumer.js";

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
