import { describe, it, expect } from "vitest";
import { TaskRunner } from "./runner.js";
import { type TaskDefinition } from "./types.js";

function task(
  id: string,
  command: string,
  overrides: Partial<TaskDefinition> = {}
): TaskDefinition {
  return {
    id,
    command,
    depends_on: [],
    retries: 0,
    timeout_ms: 5000,
    ...overrides,
  };
}

describe("TaskRunner", () => {
  describe("invocation id", () => {
    it("should generate a deterministic id run_<workflowId>_<taskId>_<attempt>", async () => {
      const runner = new TaskRunner();
      const result = await runner.run(task("my-task", "echo 1"), 2, "wf-42");
      expect(result.invocationId).toBe("run_wf-42_my-task_2");
    });

    it("should use the first attempt as attempt 1", async () => {
      const runner = new TaskRunner();
      const result = await runner.run(task("task-a", "echo 1"), 1, "wf-x");
      expect(result.invocationId).toBe("run_wf-x_task-a_1");
    });
  });

  describe("successful execution", () => {
    it("should exit with code 0 and report COMPLETED status", async () => {
      const runner = new TaskRunner();
      const result = await runner.run(task("ok", "echo hello"), 1, "wf-1");

      expect(result.status).toBe("COMPLETED");
      expect(result.exitCode).toBe(0);
      expect(result.timedOut).toBe(false);
      expect(result.error).toBeNull();
    });

    it("should capture stdout", async () => {
      const runner = new TaskRunner();
      const result = await runner.run(task("out", "echo hello world"), 1, "wf-1");

      expect(result.stdout).toContain("hello world");
    });

    it("should capture stderr on error output", async () => {
      const runner = new TaskRunner();
      const result = await runner.run(
        task("err", 'node -e "console.error(\\"stderr-msg\\")"'),
        1,
        "wf-1"
      );

      expect(result.stderr).toContain("stderr-msg");
      expect(result.stdout).toBe("");
    });

    it("should capture both stdout and stderr in separate streams", async () => {
      const runner = new TaskRunner();
      const command = 'node -e "console.log(\\"out\\"); console.error(\\"err\\")"';

      const result = await runner.run(task("both", command), 1, "wf-1");

      expect(result.stdout).toContain("out");
      expect(result.stderr).toContain("err");
    });
  });

  describe("failed execution", () => {
    it("should exit with non-zero code and report FAILED status", async () => {
      const runner = new TaskRunner();
      const result = await runner.run(
        task("fail", 'node -e "process.exit(3)"'),
        1,
        "wf-1"
      );

      expect(result.status).toBe("FAILED");
      expect(result.exitCode).toBe(3);
      expect(result.timedOut).toBe(false);
    });

    it("should capture stderr from a failing command", async () => {
      const runner = new TaskRunner();
      const result = await runner.run(
        task("fail-err", 'node -e "console.error(\\"boom\\"); process.exit(1)"'),
        1,
        "wf-1"
      );

      expect(result.stderr).toContain("boom");
      expect(result.exitCode).toBe(1);
    });
  });

  describe("timeout guard", () => {
    it("should kill the process and report timedOut=true when timeout is exceeded", async () => {
      const runner = new TaskRunner();
      const taskDef = task(
        "hung",
        'node -e "setInterval(() => {}, 1000)"',
        { timeout_ms: 150 }
      );

      const result = await runner.run(taskDef, 1, "wf-1");

      expect(result.timedOut).toBe(true);
      expect(result.status).toBe("FAILED");
    });

    it("should not time out a fast command", async () => {
      const runner = new TaskRunner();
      const result = await runner.run(
        task("fast", "echo fast"),
        1,
        "wf-1"
      );

      expect(result.timedOut).toBe(false);
      expect(result.status).toBe("COMPLETED");
    });
  });

  describe("log streaming", () => {
    it("should invoke onStdout for each output chunk", async () => {
      const runner = new TaskRunner();
      const chunks: string[] = [];
      const taskDef = task(
        "stream",
        'node -e "console.log(\\"chunk1\\"); console.log(\\"chunk2\\")"'
      );

      const result = await runner.run(taskDef, 1, "wf-1", {
        onStdout: (chunk) => chunks.push(chunk.trim()),
      });

      expect(chunks.some((c) => c.includes("chunk1"))).toBe(true);
      expect(result.stdout).toContain("chunk1");
    });

    it("should invoke onStderr for each output chunk", async () => {
      const runner = new TaskRunner();
      const chunks: string[] = [];
      const taskDef = task(
        "stream-err",
        'node -e "console.error(\\"err\\")"'
      );

      const result = await runner.run(taskDef, 1, "wf-1", {
        onStderr: (chunk) => chunks.push(chunk.trim()),
      });

      expect(chunks.some((c) => c.includes("err"))).toBe(true);
      expect(result.stderr).toContain("err");
    });

    it("should call onStdout and onStderr separately when applicable", async () => {
      const runner = new TaskRunner();
      const stdoutChunks: string[] = [];
      const stderrChunks: string[] = [];
      const taskDef = task(
        "both-stream",
        'node -e "console.log(\\"out\\"); console.error(\\"err\\")"'
      );

      const result = await runner.run(taskDef, 1, "wf-1", {
        onStdout: (c) => stdoutChunks.push(c.trim()),
        onStderr: (c) => stderrChunks.push(c.trim()),
      });

      expect(stdoutChunks.some((c) => c.includes("out"))).toBe(true);
      expect(stderrChunks.some((c) => c.includes("err"))).toBe(true);
      expect(result.stdout).toContain("out");
      expect(result.stderr).toContain("err");
    });
  });

  describe("error handling", () => {
    it("should handle spawn failure with a non-zero exit", async () => {
      const runner = new TaskRunner();
      const result = await runner.run(
        task("broken", "nonexistent-cmd-xyz-404"),
        1,
        "wf-1"
      );

      expect(result.exitCode).toBeGreaterThan(0);
      expect(result.status).toBe("FAILED");
    });

    it("should report null error when the process exits normally", async () => {
      const runner = new TaskRunner();
      const result = await runner.run(task("ok", "echo hi"), 1, "wf-1");

      expect(result.error).toBeNull();
    });
  });
});