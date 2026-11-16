import { describe, it, expect } from "vitest";
import {
  buildInvocationId,
  decodeInvocationOutcome,
  decideInvocationClaim,
  encodeInvocationOutcome,
  hasRetryBudget,
  invocationKey,
  invocationOutcomeKey,
} from "./invocation.js";

describe("buildInvocationId", () => {
  it("builds a deterministic run_<workflowId>_<taskId>_<attempt> id", () => {
    expect(buildInvocationId("wf-42", "my-task", 2)).toBe(
      "run_wf-42_my-task_2"
    );
  });

  it("distinguishes attempts of the same task", () => {
    const first = buildInvocationId("w1", "t1", 1);
    const second = buildInvocationId("w1", "t1", 2);
    expect(first).not.toBe(second);
  });

  it("distinguishes the same task id across workflows", () => {
    const a = buildInvocationId("w1", "t1", 1);
    const b = buildInvocationId("w2", "t1", 1);
    expect(a).not.toBe(b);
  });
});

describe("invocationKey", () => {
  it("namespaces the key under the invocation id", () => {
    expect(invocationKey("run_w1_t1_1")).toBe("invocation:run_w1_t1_1");
  });
});

describe("decideInvocationClaim", () => {
  it("acquires when SET NX reported OK", () => {
    expect(decideInvocationClaim("OK")).toBe("ACQUIRE");
  });

  it("treats a null reply (key already set) as a duplicate", () => {
    expect(decideInvocationClaim(null)).toBe("DUPLICATE");
  });
});

describe("hasRetryBudget", () => {
  it("allows a retry while the attempt is below the budget", () => {
    expect(hasRetryBudget(1, 2)).toBe(true);
    expect(hasRetryBudget(1, 1)).toBe(false);
  });

  it("refuses to retry once the attempt reaches the budget", () => {
    expect(hasRetryBudget(2, 2)).toBe(false);
    expect(hasRetryBudget(3, 1)).toBe(false);
  });

  it("never retries a task with no retries configured", () => {
    expect(hasRetryBudget(1, 0)).toBe(false);
  });
});

describe("invocation outcome records", () => {
  const OUTCOME = {
    status: "FAILED" as const,
    exitCode: 124,
    durationMs: 1000,
    timedOut: true,
    error: "timed out",
    stdout: "partial output",
    stderr: "timed out",
  };

  it("namespaces the outcome key under the invocation id", () => {
    expect(invocationOutcomeKey("run_w1_t1_1")).toBe(
      "invocation:run_w1_t1_1:outcome"
    );
  });

  it("round-trips an outcome", () => {
    expect(
      decodeInvocationOutcome(encodeInvocationOutcome(OUTCOME))
    ).toEqual(OUTCOME);
  });

  it("round-trips an outcome with no failure detail", () => {
    const clean = {
      status: "COMPLETED" as const,
      exitCode: 0,
      durationMs: 5,
      timedOut: false,
      error: "",
      stdout: "hello",
      stderr: "",
    };

    expect(decodeInvocationOutcome(encodeInvocationOutcome(clean))).toEqual(
      clean
    );
  });

  it("treats an absent record as no outcome", () => {
    expect(decodeInvocationOutcome(null)).toBeNull();
  });

  it("returns null for a record that is not JSON", () => {
    expect(decodeInvocationOutcome("not-json")).toBeNull();
  });

  it("returns null for a record that does not satisfy the contract", () => {
    expect(
      decodeInvocationOutcome(
        JSON.stringify({ status: "MAYBE", exitCode: 0, durationMs: 1, timedOut: false })
      )
    ).toBeNull();
  });
});
