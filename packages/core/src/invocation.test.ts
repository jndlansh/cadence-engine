import { describe, it, expect } from "vitest";
import {
  buildInvocationId,
  decideInvocationClaim,
  invocationKey,
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
