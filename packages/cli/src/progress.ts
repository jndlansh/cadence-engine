import { MultiBar, Presets, type SingleBar } from "cli-progress";
import type { TaskRunRecord, WorkflowStateSnapshot } from "@cadence/core";
import type { WorkflowStateStore } from "@cadence/orchestrator";

/** A digest of a workflow snapshot, ready to render. */
export interface ProgressView {
  total: number;
  pending: number;
  running: number;
  completed: number;
  failed: number;
  /** Fraction of tasks that have finished (completed + failed), in [0, 1]. */
  percent: number;
  /** Human-readable summary line, e.g. "3/6 done · 2 running · 1 failed". */
  label: string;
}

/**
 * Reduce a snapshot to display counts. Pure and deterministic, so the progress
 * math is unit-testable without a TTY, a bar, or Redis.
 */
export function buildProgressView(
  snapshot: WorkflowStateSnapshot
): ProgressView {
  const records = Object.values(snapshot.tasks);
  const count = (status: TaskRunRecord["status"]): number =>
    records.filter((r) => r.status === status).length;

  const running = count("RUNNING");
  const completed = count("COMPLETED");
  const failed = count("FAILED");
  const pending = records.length - running - completed - failed;
  const total = records.length;
  const percent = total === 0 ? 0 : (completed + failed) / total;

  const parts = [`${completed + failed}/${total} done`];
  if (running > 0) parts.push(`${running} running`);
  if (failed > 0) parts.push(`${failed} failed`);
  if (pending > 0) parts.push(`${pending} pending`);

  return {
    total,
    pending,
    running,
    completed,
    failed,
    percent,
    label: parts.join(" · "),
  };
}

/** Renders progress updates; two implementations: animated and plain. */
export interface ProgressReporter {
  update(snapshot: WorkflowStateSnapshot): void;
  /** Print a line above the progress area (or inline, when plain). */
  log(line: string): void;
  stop(): void;
  /** True when the reporter draws an in-place bar (and owns the terminal line). */
  readonly animated: boolean;
}

/** Non-TTY / `--no-progress`: one line per count change, nothing escapes. */
export class PlainProgressReporter implements ProgressReporter {
  readonly animated = false;
  private lastLabel = "";

  update(snapshot: WorkflowStateSnapshot): void {
    const view = buildProgressView(snapshot);
    if (view.label !== this.lastLabel) {
      this.lastLabel = view.label;
      process.stdout.write(`[cadence] ${view.label}\n`);
    }
  }

  log(line: string): void {
    process.stdout.write(`${line}\n`);
  }

  stop(): void {}
}

/**
 * In-place bar backed by cli-progress. A `MultiBar` with a single bar is used
 * (rather than `SingleBar`) because only MultiBar can print lines above the bar
 * via `log()`, which is what keeps the orchestrator's own output from tearing
 * the bar.
 */
export class BarProgressReporter implements ProgressReporter {
  readonly animated = true;
  private readonly multi: MultiBar;
  private line: SingleBar | null = null;

  constructor(stream: NodeJS.WriteStream = process.stdout) {
    this.multi = new MultiBar(
      {
        format: " {bar} {percentage}% | {value}/{total} | {label}",
        stream,
        hideCursor: true,
        clearOnComplete: false,
      },
      Presets.shades_classic
    );
  }

  update(snapshot: WorkflowStateSnapshot): void {
    const view = buildProgressView(snapshot);
    const value = view.completed + view.failed;

    if (!this.line) {
      this.line = this.multi.create(view.total, value, { label: view.label });
    } else {
      this.line.update(value, { label: view.label });
    }
  }

  log(line: string): void {
    this.multi.log(`${line}\n`);
  }

  stop(): void {
    this.multi.stop();
  }
}

/**
 * Pick an animated bar only when asked AND attached to a real terminal; piped
 * output always gets plain lines.
 */
export function createProgressReporter(
  enabled: boolean,
  stream: NodeJS.WriteStream = process.stdout
): ProgressReporter {
  return enabled && Boolean(stream.isTTY)
    ? new BarProgressReporter(stream)
    : new PlainProgressReporter();
}

/**
 * Route `console.log/warn/error` through the reporter so the orchestrator's
 * output is printed above the bar instead of corrupting it. Returns a restore
 * function. Only needed for the animated reporter.
 */
export function bridgeConsole(reporter: ProgressReporter): () => void {
  const original = {
    log: console.log,
    warn: console.warn,
    error: console.error,
  };
  const forward = (...args: unknown[]): void =>
    reporter.log(args.map((a) => String(a)).join(" "));

  console.log = forward;
  console.warn = forward;
  console.error = forward;

  return () => {
    console.log = original.log;
    console.warn = original.warn;
    console.error = original.error;
  };
}

/**
 * Decorator over a `WorkflowStateStore` that repaints on every save. Keeps the
 * progress bar in lockstep with the persisted lifecycle without the orchestrator
 * knowing anything about rendering.
 */
export class ProgressStateStore implements WorkflowStateStore {
  constructor(
    private readonly inner: WorkflowStateStore,
    private readonly reporter: ProgressReporter
  ) {}

  async save(snapshot: WorkflowStateSnapshot): Promise<void> {
    await this.inner.save(snapshot);
    this.reporter.update(snapshot);
  }

  load(workflowId: string): Promise<WorkflowStateSnapshot | null> {
    return this.inner.load(workflowId);
  }

  disconnect(): Promise<void> {
    return this.inner.disconnect();
  }
}
