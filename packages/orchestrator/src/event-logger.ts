import { appendFile } from "node:fs/promises";

/**
 * A structured lifecycle/telemetry record. Emitted for every state transition
 * (and, through `detail`, the outcome of each) so a run can be replayed or
 * grepped long after the process that produced it is gone.
 */
export interface WorkflowEvent {
  /** ISO-8601 timestamp of the event. */
  ts: string;
  workflowId: string;
  /** Lifecycle event type, e.g. WORKFLOW_STARTED / TASK_COMPLETED. */
  type: string;
  /** Task ids the event concerns, when it concerns any. */
  taskIds?: string[];
  /** Event-specific extras (status, attempt, duration, …). */
  detail?: Record<string, unknown>;
}

/**
 * Port: something that accepts structured lifecycle events. The orchestrator
 * depends on this, so its DAG walk stays testable with a fake logger.
 */
export interface WorkflowEventSink {
  emit(event: WorkflowEvent): Promise<void>;
  close(): Promise<void>;
}

export interface WorkflowEventLoggerOptions {
  /** Append JSON lines here when set. Omit for console-only logging. */
  logFile?: string;
  /** Print a compact one-line summary. Defaults to true. */
  toConsole?: boolean;
}

/**
 * Structured event logger: a human-readable line on the console plus, when a
 * file is configured, JSON-lines for machine consumption.
 *
 * Deliberately simple — one append per event, awaited by the caller so a
 * failure surfaces at the emit site rather than vanishing in a buffer.
 */
export class WorkflowEventLogger implements WorkflowEventSink {
  private readonly logFile?: string;
  private readonly toConsole: boolean;

  constructor(options?: WorkflowEventLoggerOptions) {
    this.logFile = options?.logFile;
    this.toConsole = options?.toConsole ?? true;
  }

  async emit(event: WorkflowEvent): Promise<void> {
    if (this.toConsole) {
      const ids =
        event.taskIds && event.taskIds.length > 0
          ? ` ${event.taskIds.join(", ")}`
          : "";
      console.log(`[wf:${event.workflowId}] ${event.type}${ids}`);
    }

    if (this.logFile) {
      await appendFile(this.logFile, `${JSON.stringify(event)}\n`, "utf-8");
    }
  }

  /** Reserved for buffered writers; appends are synchronous-per-event today. */
  async close(): Promise<void> {}
}
