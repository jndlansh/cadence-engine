import { spawn, type ChildProcess } from "node:child_process";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const READY_PATTERN = /WORKER_READY id=(\S+)/;
const DEFAULT_READY_TIMEOUT_MS = 15_000;

/** Extract the worker id from a `WORKER_READY id=<id>` line, or null. */
export function parseWorkerReady(line: string): string | null {
  const match = line.match(READY_PATTERN);
  return match ? match[1] : null;
}

/**
 * Repo root, resolved from this module's location so worker spawning does not
 * depend on the caller's working directory.
 * `packages/cli/src/workers.ts` -> up three -> repo root.
 */
export function resolveRepoRoot(): string {
  const here = dirname(fileURLToPath(import.meta.url));
  return resolve(here, "..", "..", "..");
}

export interface SpawnWorkersOptions {
  count: number;
  redisUrl: string;
  /** Extra env for the worker children (heartbeat/backoff tuning). */
  env?: Record<string, string>;
  readyTimeoutMs?: number;
}

export interface WorkerPool {
  workerIds: string[];
  /** Terminate every child and resolve once they have exited. */
  stop(): Promise<void>;
}

function waitForExit(child: ChildProcess): Promise<void> {
  return new Promise((resolveExit) => {
    if (child.exitCode !== null || child.signalCode !== null) {
      resolveExit();
      return;
    }
    child.once("exit", () => resolveExit());
  });
}

/**
 * Spawn `count` worker child processes running `scripts/worker-node.ts`, and
 * resolve once every one has announced `WORKER_READY`. Worker stdout is
 * captured (not forwarded) so it cannot corrupt the progress bar; stderr is
 * forwarded so failures are still visible.
 */
export async function spawnWorkers(
  options: SpawnWorkersOptions
): Promise<WorkerPool> {
  const repoRoot = resolveRepoRoot();
  const workerScript = join(repoRoot, "scripts", "worker-node.ts");
  const readyTimeoutMs = options.readyTimeoutMs ?? DEFAULT_READY_TIMEOUT_MS;

  const children: ChildProcess[] = [];

  const ready = Array.from({ length: options.count }, () =>
    new Promise<string>((resolveId, rejectId) => {
      const child = spawn(process.execPath, ["--import", "tsx", workerScript], {
        cwd: repoRoot,
        env: {
          ...process.env,
          REDIS_URL: options.redisUrl,
          ...options.env,
        },
        stdio: ["ignore", "pipe", "pipe"],
      });
      children.push(child);

      let buffer = "";
      const timer = setTimeout(() => {
        rejectId(
          new Error(
            `worker did not announce WORKER_READY within ${readyTimeoutMs}ms`
          )
        );
      }, readyTimeoutMs);

      child.stdout?.on("data", (chunk: Buffer) => {
        buffer += chunk.toString("utf-8");
        const id = parseWorkerReady(buffer);
        if (id) {
          clearTimeout(timer);
          resolveId(id);
        }
      });

      child.stderr?.on("data", (chunk: Buffer) => {
        process.stderr.write(`[worker] ${chunk.toString("utf-8")}`);
      });

      child.once("exit", (code, signal) => {
        clearTimeout(timer);
        rejectId(
          new Error(
            `worker exited (code=${code} signal=${signal}) before it was ready`
          )
        );
      });
    })
  );

  const workerIds = await Promise.all(ready);

  return {
    workerIds,
    async stop(): Promise<void> {
      for (const child of children) {
        if (child.exitCode === null && child.signalCode === null) {
          child.kill("SIGTERM");
        }
      }
      await Promise.all(children.map(waitForExit));
    },
  };
}
