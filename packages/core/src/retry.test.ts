import { describe, it, expect } from "vitest";
import {
  computeBackoffDelayMs,
  decodeRetryPayload,
  encodeRetryPayload,
  DEFAULT_RETRY_BASE_DELAY_MS,
  DEFAULT_RETRY_MAX_DELAY_MS,
} from "./retry.js";
import type { RetryPayload } from "./types.js";

const PAYLOAD: RetryPayload = {
  workflowId: "w1",
  taskId: "t1",
  command: "echo hi",
  timeoutMs: 5000,
  retries: 3,
  attempt: 2,
};

describe("computeBackoffDelayMs", () => {
  it("waits one base delay for the first retry", () => {
    expect(computeBackoffDelayMs(1, { baseDelayMs: 1000 })).toBe(1000);
  });

  it("doubles the delay with each successive retry", () => {
    expect(computeBackoffDelayMs(2, { baseDelayMs: 1000 })).toBe(2000);
    expect(computeBackoffDelayMs(3, { baseDelayMs: 1000 })).toBe(4000);
    expect(computeBackoffDelayMs(4, { baseDelayMs: 1000 })).toBe(8000);
  });

  it("caps the delay at maxDelayMs", () => {
    expect(
      computeBackoffDelayMs(10, { baseDelayMs: 1000, maxDelayMs: 5000 })
    ).toBe(5000);
  });

  it("treats a non-positive retry number as the first retry", () => {
    expect(computeBackoffDelayMs(0, { baseDelayMs: 1000 })).toBe(1000);
    expect(computeBackoffDelayMs(-5, { baseDelayMs: 1000 })).toBe(1000);
  });

  it("does not overflow past the cap for a huge retry number", () => {
    expect(computeBackoffDelayMs(999)).toBe(DEFAULT_RETRY_MAX_DELAY_MS);
  });

  it("falls back to the documented defaults", () => {
    expect(computeBackoffDelayMs(1)).toBe(DEFAULT_RETRY_BASE_DELAY_MS);
  });
});

describe("retry payload encoding", () => {
  it("round-trips a payload", () => {
    expect(decodeRetryPayload(encodeRetryPayload(PAYLOAD))).toEqual(PAYLOAD);
  });

  it("produces a stable member for equal payloads", () => {
    expect(encodeRetryPayload(PAYLOAD)).toBe(
      encodeRetryPayload({ ...PAYLOAD })
    );
  });

  it("distinguishes attempts of the same task", () => {
    expect(encodeRetryPayload(PAYLOAD)).not.toBe(
      encodeRetryPayload({ ...PAYLOAD, attempt: 3 })
    );
  });

  it("returns null for non-JSON input", () => {
    expect(decodeRetryPayload("not json")).toBeNull();
  });

  it("returns null for a JSON object missing required fields", () => {
    expect(decodeRetryPayload(JSON.stringify({ taskId: "t1" }))).toBeNull();
  });
});
