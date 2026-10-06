import { spawn, execSync, type ChildProcess } from "node:child_process";
import {
  type TaskDefinition,
  type TaskExecutionResult,
  type TaskRunnerOptions,
  type TaskExecutionStatus,
} from "./types.js";

const SIGNAL_EXIT_BASE = 128;

const SIGNAL_NUMBERS: Record<string, number> = {
  SIGHUP: 1,
  SIGINT: 2,
  SIGQUIT: 3,
  SIGILL: 4,
  SIGTRAP: 5,
  SIGABRT: 6,
  SIGBUS: 7,
  SIGFPE: 8,
  SIGKILL: 9,
  SIGSEGV: 11,
  SIGPIPE: 13,
  SIGTERM: 15,
};

function signalToExitCode(signal: NodeJS.Signals): number {
  return SIGNAL_NUMBERS[signal] ? SIGNAL_EXIT_BASE + SIGNAL_NUMBERS[signal] : 1;
}

function spawnCommand(command: string): ChildProcess {
  if (process.platform === "win32") {
    return spawn("cmd.exe", ["/c", command], {
      stdio: ["ignore", "pipe", "pipe"],
      windowsVerbatimArguments: true,
    });
  }
  return spawn("sh", ["-c", command], {
    stdio: ["ignore", "pipe", "pipe"],
  });
}

function killProcess(child: ChildProcess): void {
  if (!child.pid) return;

  if (process.platform === "win32") {
    try {
      execSync(`taskkill /pid ${child.pid} /T /F 2>nul`);
    } catch {
      // process might have already exited
    }
  } else {
    child.kill("SIGKILL");
  }
}

export class TaskRunner {
  public async run(
    task: TaskDefinition,
    attempt: number,
    workflowId: string,
    options?: TaskRunnerOptions
  ): Promise<TaskExecutionResult> {
    const invocationId = `run_${workflowId}_${task.id}_${attempt}`;
    const startTime = Date.now();

    const child = spawnCommand(task.command);

    let stdout = "";
    let stderr = "";
    let timedOut = false;

    return new Promise((resolve) => {
      let timeoutHandle: NodeJS.Timeout | null = null;

      if (task.timeout_ms > 0) {
        timeoutHandle = setTimeout(() => {
          timedOut = true;
          killProcess(child);
        }, task.timeout_ms);
      }

      child.stdout?.on("data", (chunk: Buffer) => {
        const text = chunk.toString("utf-8");
        stdout += text;
        options?.onStdout?.(text);
      });

      child.stderr?.on("data", (chunk: Buffer) => {
        const text = chunk.toString("utf-8");
        stderr += text;
        options?.onStderr?.(text);
      });

      // 'close' event guarantees stdio streams have completely closed and flushed
      child.on("close", (code: number | null, signal: NodeJS.Signals | null) => {
        if (timeoutHandle) clearTimeout(timeoutHandle);

        const durationMs = Date.now() - startTime;

        let exitCode: number;
        if (timedOut) {
          exitCode = 124; // Standard POSIX timeout exit code convention
        } else if (code !== null) {
          exitCode = code;
        } else if (signal !== null) {
          exitCode = signalToExitCode(signal);
        } else {
          exitCode = 1;
        }

        const status: TaskExecutionStatus =
          exitCode === 0 && !timedOut ? "COMPLETED" : "FAILED";

        resolve({
          taskId: task.id,
          invocationId,
          status,
          exitCode,
          stdout: stdout.trim(),
          stderr: stderr.trim(),
          timedOut,
          durationMs,
          error: null,
        });
      });

      child.on("error", (err: Error) => {
        if (timeoutHandle) clearTimeout(timeoutHandle);

        resolve({
          taskId: task.id,
          invocationId,
          status: "FAILED",
          exitCode: 1,
          stdout: stdout.trim(),
          stderr: (stderr + "\n" + err.message).trim(),
          timedOut: false,
          durationMs: Date.now() - startTime,
          error: err,
        });
      });
    });
  }
}