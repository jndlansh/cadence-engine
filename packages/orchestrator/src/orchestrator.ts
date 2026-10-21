import {
  type ParsedWorkflow,
  type TaskCompletion,
  type TaskDefinition,
} from "@cadence/core";

/**
 * Port: something that can publish task definitions onto the queue.
 * `TaskDispatcher` satisfies this structurally.
 */
export interface TaskSink {
  dispatchTasks(
    workflowId: string,
    tasks: TaskDefinition[],
    attempt?: number
  ): Promise<string[]>;
}

/**
 * Port: a blocking source of terminal task outcomes.
 */
export interface CompletionSource {
  /** Blocks briefly. Resolves with new completions, or null if none arrived. */
  read(): Promise<TaskCompletion[] | null>;
}

export type WorkflowRunStatus = "COMPLETED" | "FAILED" | "STALLED";

export type OrchestrationEventType = "DISPATCHED" | "COMPLETED" | "FAILED";

/**
 * Ordered record of what the orchestrator did, used for logging and for
 * asserting the topological invariant in tests and the live demo.
 */
export interface OrchestrationEvent {
  type: OrchestrationEventType;
  taskIds: string[];
}

export interface WorkflowRunResult {
  workflowId: string;
  status: WorkflowRunStatus;
  completed: string[];
  failed: string[];
  events: OrchestrationEvent[];
}

export interface OrchestratorOptions {
  /**
   * How long to wait without any completion before reporting the run as
   * STALLED instead of blocking forever. Without a PEL reclaimer (Phase 4), a
   * worker killed mid-task produces no completion at all, so this guard is what
   * makes that condition observable rather than a silent deadlock.
   */
  completionTimeoutMs?: number;
}

const DEFAULT_COMPLETION_TIMEOUT_MS = 30_000;

/**
 * Walks a workflow DAG at runtime: dispatch the initial wave, observe
 * completions, dispatch newly unblocked children, repeat.
 *
 * Depends only on the two ports above, so the scheduling logic is unit-testable
 * without a live Redis.
 */
export class WorkflowOrchestrator {
  private readonly sink: TaskSink;
  private readonly source: CompletionSource;
  private readonly completionTimeoutMs: number;

  constructor(
    sink: TaskSink,
    source: CompletionSource,
    options?: OrchestratorOptions
  ) {
    this.sink = sink;
    this.source = source;
    this.completionTimeoutMs =
      options?.completionTimeoutMs ?? DEFAULT_COMPLETION_TIMEOUT_MS;
  }

  async run(
    workflowId: string,
    workflow: ParsedWorkflow
  ): Promise<WorkflowRunResult> {
    const resolver = workflow.resolver;
    const total = workflow.definition.tasks.length;

    const dispatched = new Set<string>();
    const completed = new Set<string>();
    const failed: string[] = [];
    const events: OrchestrationEvent[] = [];

    const buildResult = (status: WorkflowRunStatus): WorkflowRunResult => ({
      workflowId,
      status,
      completed: [...completed].sort(),
      failed: [...failed],
      events,
    });

    // `getNextExecutableTasks` returns every task whose parents are complete,
    // and `[].every(...)` is vacuously true, so dependency-free roots come back
    // on every call. The `dispatched` filter is what stops double-dispatch.
    const dispatch = async (candidates: TaskDefinition[]): Promise<void> => {
      const fresh = candidates.filter((t) => !dispatched.has(t.id));
      if (fresh.length === 0) return;

      const ids = await this.sink.dispatchTasks(workflowId, fresh, 1);
      for (const task of fresh) {
        dispatched.add(task.id);
      }
      const taskIds = fresh.map((t) => t.id);
      events.push({ type: "DISPATCHED", taskIds });
      console.log(
        `[orchestrator] dispatched wave [${taskIds.join(", ")}] (${ids.length} entries)`
      );
    };

    await dispatch(resolver.getInitialTasks());

    let lastProgressAt = Date.now();

    for (;;) {
      if (completed.size === total) {
        return buildResult("COMPLETED");
      }

      const inFlight = dispatched.size - completed.size - failed.length;

      if (failed.length > 0 && inFlight === 0) {
        return buildResult("FAILED");
      }

      if (Date.now() - lastProgressAt >= this.completionTimeoutMs) {
        console.warn(
          `[orchestrator] no progress for ${this.completionTimeoutMs}ms with ` +
            `${inFlight} task(s) in flight — reporting STALLED`
        );
        return buildResult("STALLED");
      }

      const batch = await this.source.read();
      if (batch === null || batch.length === 0) {
        continue;
      }

      for (const completion of batch) {
        // Ignore foreign workflows and replays of tasks we never dispatched.
        if (completion.workflowId !== workflowId) continue;
        if (!dispatched.has(completion.taskId)) continue;
        // Idempotent: at-least-once delivery can duplicate a completion.
        if (
          completed.has(completion.taskId) ||
          failed.includes(completion.taskId)
        ) {
          continue;
        }

        lastProgressAt = Date.now();

        if (completion.status === "COMPLETED") {
          completed.add(completion.taskId);
          events.push({ type: "COMPLETED", taskIds: [completion.taskId] });
          console.log(`[orchestrator] completed ${completion.taskId}`);

          // Fail-fast: once anything has failed, never unblock more work.
          if (failed.length === 0) {
            await dispatch(resolver.getNextExecutableTasks(completed));
          }
        } else {
          failed.push(completion.taskId);
          events.push({ type: "FAILED", taskIds: [completion.taskId] });
          console.error(
            `[orchestrator] task ${completion.taskId} failed terminally ` +
              `(exit=${completion.exitCode}) — aborting run`
          );
        }
      }
    }
  }
}
