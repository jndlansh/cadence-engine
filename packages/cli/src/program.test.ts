import { describe, it, expect } from "vitest";
import { CommanderError, type Command } from "commander";
import { createProgram, type Handlers } from "./program.js";
import type { RunOptions } from "./commands/run.js";

interface Captured {
  run?: RunOptions;
  status?: { id: string; url: string };
}

function harness(): { program: Command; calls: Captured } {
  const calls: Captured = {};
  const handlers: Handlers = {
    run: async (options) => {
      calls.run = options;
      return 0;
    },
    status: async (id, url) => {
      calls.status = { id, url };
      return 0;
    },
  };

  const program = createProgram(handlers);
  // Keep commander's error text out of the test output (subcommands too).
  program.configureOutput({ writeErr: () => {} });
  for (const command of program.commands) {
    command.configureOutput({ writeErr: () => {} });
  }
  return { program, calls };
}

const DEFAULT_REDIS = "redis://127.0.0.1:6379";

describe("createProgram", () => {
  it("parses `run <file>` with defaults", async () => {
    const { program, calls } = harness();

    await program.parseAsync(["node", "cadence", "run", "wf.yaml"]);

    expect(calls.run).toEqual({
      file: "wf.yaml",
      workers: 1,
      workflowId: undefined,
      timeoutMs: 30000,
      logFile: undefined,
      progress: true,
      redisUrl: DEFAULT_REDIS,
    });
  });

  it("parses explicit run options and --no-progress", async () => {
    const { program, calls } = harness();

    await program.parseAsync([
      "node",
      "cadence",
      "run",
      "wf.yaml",
      "--workers",
      "3",
      "--workflow-id",
      "wf1",
      "--timeout",
      "5000",
      "--log",
      "run.log",
      "--no-progress",
      "--redis-url",
      "redis://localhost:7000",
    ]);

    expect(calls.run).toEqual({
      file: "wf.yaml",
      workers: 3,
      workflowId: "wf1",
      timeoutMs: 5000,
      logFile: "run.log",
      progress: false,
      redisUrl: "redis://localhost:7000",
    });
  });

  it("routes `status <workflow-id>` with the redis url", async () => {
    const { program, calls } = harness();

    await program.parseAsync(["node", "cadence", "status", "wf1"]);

    expect(calls.status).toEqual({ id: "wf1", url: DEFAULT_REDIS });
  });

  it("rejects an unknown command", async () => {
    const { program } = harness();

    await expect(
      program.parseAsync(["node", "cadence", "bogus"])
    ).rejects.toThrow();
  });

  it("rejects a missing required argument with a commander error", async () => {
    const { program } = harness();

    // Subcommands must inherit the exit override, or this exits the process.
    await expect(
      program.parseAsync(["node", "cadence", "status"])
    ).rejects.toBeInstanceOf(CommanderError);
  });
});
