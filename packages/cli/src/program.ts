import { Command, type OptionValues } from "commander";
import type { RunOptions } from "./commands/run.js";

export interface Handlers {
  run: (options: RunOptions) => Promise<number>;
  status: (workflowId: string, redisUrl: string) => Promise<number>;
}

function str(value: OptionValues[string]): string | undefined {
  return typeof value === "string" ? value : undefined;
}

/**
 * Build the commander program. Actions delegate to injected `handlers`, so the
 * argument parsing can be tested without running anything.
 */
export function createProgram(handlers: Handlers): Command {
  const program = new Command();

  program
    .name("cadence")
    .description("Distributed, fault-tolerant DAG workflow orchestration engine")
    .version("1.0.0");

  program
    .command("run")
    .description("Run a workflow YAML file")
    .argument("<file>", "path to the workflow YAML")
    .option("-w, --workers <count>", "number of local worker processes", "1")
    .option(
      "--workflow-id <id>",
      "explicit workflow id (default: <name>_<timestamp>)"
    )
    .option(
      "--timeout <ms>",
      "stall timeout with no completion, in ms",
      "30000"
    )
    .option("--log <file>", "also write a JSON-lines event log")
    .option("--no-progress", "disable the live progress bar")
    .option(
      "--redis-url <url>",
      "Redis connection string",
      "redis://127.0.0.1:6379"
    )
    .action(async (file: string, options: OptionValues): Promise<void> => {
      process.exitCode = await handlers.run({
        file,
        workers: Number(options.workers),
        workflowId: str(options.workflowId),
        timeoutMs: Number(options.timeout),
        logFile: str(options.log),
        progress: options.progress !== false,
        redisUrl: str(options.redisUrl) ?? "redis://127.0.0.1:6379",
      });
    });

  program
    .command("status")
    .description("Print the persisted lifecycle state of a workflow")
    .argument("<workflow-id>", "the workflow id to inspect")
    .option(
      "--redis-url <url>",
      "Redis connection string",
      "redis://127.0.0.1:6379"
    )
    .action(
      async (workflowId: string, options: OptionValues): Promise<void> => {
        process.exitCode = await handlers.status(
          workflowId,
          str(options.redisUrl) ?? "redis://127.0.0.1:6379"
        );
      }
    );

  // Subcommands do not inherit the program's exit override, so a bad invocation
  // (e.g. a missing argument) would call process.exit(1) directly. Apply it to
  // every command so commander throws and `main` can map the exit code.
  program.exitOverride();
  for (const command of program.commands) {
    command.exitOverride();
  }

  return program;
}
