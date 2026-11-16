import { describe, it, expect } from "vitest";
import {
  MAX_PERSISTED_OUTPUT_CHARS,
  OUTPUT_MARKER_ALLOWANCE,
  boundReceivedOutput,
  truncateOutput,
} from "./output.js";

describe("truncateOutput", () => {
  it("returns short output untouched", () => {
    expect(truncateOutput("hello")).toBe("hello");
  });

  it("keeps output exactly at the limit", () => {
    const exact = "x".repeat(10);
    expect(truncateOutput(exact, 10)).toBe(exact);
  });

  it("keeps the head and says how much was dropped", () => {
    const result = truncateOutput("abcdefghij", 4);

    expect(result).toBe("abcd…[output truncated: 6 more characters]");
    expect(result.startsWith("abcd")).toBe(true);
  });

  it("never silently looks complete when it is not", () => {
    const long = "y".repeat(MAX_PERSISTED_OUTPUT_CHARS + 1);
    const result = truncateOutput(long);

    expect(result.startsWith("y".repeat(MAX_PERSISTED_OUTPUT_CHARS))).toBe(true);
    expect(result).toContain("output truncated: 1 more characters");
  });

  it("preserves an empty capture", () => {
    expect(truncateOutput("")).toBe("");
  });
});

describe("boundReceivedOutput", () => {
  it("leaves an absent capture absent", () => {
    expect(boundReceivedOutput(undefined)).toBeUndefined();
  });

  it("stores a producer's bounded capture verbatim", () => {
    const cut = truncateOutput("z".repeat(9000));

    // The count must survive: re-cutting would claim 40 characters were dropped
    // when the producer dropped thousands.
    expect(boundReceivedOutput(cut)).toBe(cut);
    expect(boundReceivedOutput(cut)).toContain("4904 more characters");
  });

  it("stores a short capture unchanged", () => {
    expect(boundReceivedOutput("hi")).toBe("hi");
  });

  it("cuts a value beyond any conforming capture", () => {
    const huge = "q".repeat(50_000);
    const bound = boundReceivedOutput(huge);

    expect(bound).toContain("output truncated");
    expect(bound?.length).toBeLessThanOrEqual(
      MAX_PERSISTED_OUTPUT_CHARS + OUTPUT_MARKER_ALLOWANCE + 64
    );
  });
});
