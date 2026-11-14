import { CommanderError } from "commander";
import { createProgram } from "./program.js";
import { runCommand } from "./commands/run.js";
import { statusCommand } from "./commands/status.js";

/**
 * CLI entry point. Maps outcomes to exit codes:
 * 0 success, 1 runtime failure, 2 usage error, 3 stalled (set by `run`).
 */
async function main(): Promise<void> {
  const program = createProgram({ run: runCommand, status: statusCommand });
  // Throw instead of calling process.exit, so we control the exit code and any
  // in-flight cleanup still runs.
  program.exitOverride();

  try {
    await program.parseAsync(process.argv);
  } catch (err) {
    if (err instanceof CommanderError) {
      // --help / --version exit cleanly; anything else is a usage error.
      process.exitCode = err.exitCode === 0 ? 0 : 2;
      return;
    }
    console.error(`[cadence] ${err instanceof Error ? err.message : String(err)}`);
    process.exitCode = 1;
  }
}

void main();
