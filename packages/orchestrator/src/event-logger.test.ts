import {
  describe,
  it,
  expect,
  vi,
  beforeEach,
  afterEach,
  type MockInstance,
} from "vitest";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WorkflowEventLogger, type WorkflowEvent } from "./event-logger.js";

const EVENT: WorkflowEvent = {
  ts: "2026-01-01T00:00:00.000Z",
  workflowId: "wf1",
  type: "TASK_COMPLETED",
  taskIds: ["a"],
  detail: { attempt: 1 },
};

describe("WorkflowEventLogger", () => {
  let logSpy: MockInstance;
  let dir: string;

  beforeEach(async () => {
    logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    dir = await mkdtemp(join(tmpdir(), "cadence-event-log-"));
  });

  afterEach(async () => {
    logSpy.mockRestore();
    await rm(dir, { recursive: true, force: true });
  });

  it("prints a compact line to the console by default", async () => {
    await new WorkflowEventLogger().emit(EVENT);
    expect(logSpy).toHaveBeenCalledWith("[wf:wf1] TASK_COMPLETED a");
  });

  it("omits the task list for an event that concerns no tasks", async () => {
    await new WorkflowEventLogger().emit({
      ts: "t",
      workflowId: "wf1",
      type: "WORKFLOW_STARTED",
    });
    expect(logSpy).toHaveBeenCalledWith("[wf:wf1] WORKFLOW_STARTED");
  });

  it("stays silent on the console when toConsole is false", async () => {
    await new WorkflowEventLogger({ toConsole: false }).emit(EVENT);
    expect(logSpy).not.toHaveBeenCalled();
  });

  it("appends one JSON line per event to the configured file", async () => {
    const file = join(dir, "run.jsonl");
    const logger = new WorkflowEventLogger({ logFile: file, toConsole: false });

    await logger.emit(EVENT);
    await logger.emit({ ts: "t2", workflowId: "wf1", type: "WORKFLOW_FINISHED" });

    const lines = (await readFile(file, "utf-8")).trim().split("\n");
    expect(lines).toHaveLength(2);
    expect(JSON.parse(lines[0])).toEqual(EVENT);
    expect(JSON.parse(lines[1]).type).toBe("WORKFLOW_FINISHED");
  });
});
