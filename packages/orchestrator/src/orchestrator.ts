import {
  initialWorkflowState,
  planWorkflowResume,
  reduceWorkflowState,
  withCursor,
  type ParsedWorkflow,
  type TaskCompletion,
  type TaskDefinition,
  type TerminalWorkflowStatus,
  type WorkflowStateEvent,
  type WorkflowStateSnapshot,
} from "@cadence/core";
import type { WorkflowStateStore } from "./state-store.js";
import type { WorkflowEventSink } from "./event-logger.js";

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
  /**
   * Stream position after the most recent `read`. The orchestrator persists it
   * only once that batch has been applied, so a crash mid-batch replays the
   * batch (harmless — completions are idempotent) rather than skipping it.
   */
  cursor(): string;
  /**
   * Position of the newest entry currently in the stream (`"0"` when the stream
   * is empty). A fresh run starts reading here, so it can never apply a
   * completion published by an earlier run that used the same workflow id.
   */
  headCursor(): Promise<string>;
  /** Restore a persisted position, so a resumed run replays from where it stopped. */
  seek(cursor: string): void;
}

export type WorkflowRunStatus = TerminalWorkflowStatus;

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
  /** Final lifecycle snapshot, persisted when a `stateStore` is configured. */
  state: WorkflowStateSnapshot;
  /** True when this run continued a persisted, non-terminal run. */
  resumed: boolean;
}

export interface WorkflowRunOptions {
  /**
   * When the id already has a *terminal* persisted snapshot, return that outcome
   * instead of starting the workflow over. A non-terminal run is always resumed
   * whether or not this is set — orphaning it is never correct.
   */
  resume?: boolean;
}

export interface OrchestratorOptions {
  /**
   * How long to wait without any completion before reporting the run as
   * STALLED instead of blocking forever. A worker killed mid-task publishes no
   * completion at all, so this guard is what makes that condition observable
   * rather than a silent deadlock.
   */
  completionTimeoutMs?: number;
  /** Durable sink for the lifecycle snapshot. Omit to keep state in-memory. */
  stateStore?: WorkflowStateStore;
  /** Structured event sink. Omit for no telemetry. */
  logger?: WorkflowEventSink;
}

const DEFAULT_COMPLETION_TIMEOUT_MS = 30_000;

/** Task ids the snapshot records as terminal-completed, sorted. */
function completedTaskIds(snapshot: WorkflowStateSnapshot): string[] {
  return Object.values(snapshot.tasks)
    .filter((record) => record.status === "COMPLETED")
    .map((record) => record.taskId)
    .sort();
}

/**
 * Walks a workflow DAG at runtime: dispatch the initial wave, observe
 * completions, dispatch newly unblocked children, repeat.
 *
 * When a `stateStore` is configured the walk is also **resumable**: a persisted,
 * non-terminal run is continued instead of restarted (see `planWorkflowResume`),
 * which is what stops an orchestrator restart from orphaning a `RUNNING`
 * workflow. Idempotency in the worker is what makes that safe.
 *
 * Depends only on the two ports above, so the scheduling logic is unit-testable
 * without a live Redis.
 */
export class WorkflowOrchestrator {
  private readonly sink: TaskSink;
  private readonly source: CompletionSource;
  private readonly completionTimeoutMs: number;
  private readonly stateStore?: WorkflowStateStore;
  private readonly logger?: WorkflowEventSink;

  constructor(
    sink: TaskSink,
    source: CompletionSource,
    options?: OrchestratorOptions
  ) {
    this.sink = sink;
    this.source = source;
    this.completionTimeoutMs =
      options?.completionTimeoutMs ?? DEFAULT_COMPLETION_TIMEOUT_MS;
    this.stateStore = options?.stateStore;
    this.logger = options?.logger;
  }

  async run(
    workflowId: string,
    workflow: ParsedWorkflow,
    runOptions?: WorkflowRunOptions
  ): Promise<WorkflowRunResult> {
    const resolver = workflow.resolver;
    const total = workflow.definition.tasks.length;
    const tasksById = new Map(
      workflow.definition.tasks.map((task) => [task.id, task])
    );

    const persisted = await this.loadPersisted(workflowId);
    const persistedState = persisted?.state;

    // A run that already finished is not silently re-run: an explicit id means
    // "run this again", while `resume: true` asks for the prior outcome instead.
    if (
      persisted &&
      (persistedState === "COMPLETED" ||
        persistedState === "FAILED" ||
        persistedState === "STALLED")
    ) {
      if (runOptions?.resume) {
        console.log(
          `[orchestrator] ${workflowId} already finished ${persistedState} — ` +
            `returning the persisted outcome`
        );
        return {
          workflowId,
          status: persistedState,
          completed: completedTaskIds(persisted),
          failed: [...persisted.failed],
          events: [],
          state: persisted,
          resumed: true,
        };
      }
      console.warn(
        `[orchestrator] ${workflowId} previously finished ${persistedState}; ` +
          `starting a fresh run. Its task attempts are already claimed, so they ` +
          `will not execute again — pass --resume to keep the existing result, ` +
          `or use a new --workflow-id to genuinely re-run it.`
      );
    }

    const plan = planWorkflowResume(persisted, workflow.definition.tasks);
    const resumed = plan.mode === "RESUME";

    const dispatched = new Set<string>();
    const completed = new Set<string>();
    const failed: string[] = [];
    const events: OrchestrationEvent[] = [];

    for (const taskId of plan.completed) {
      completed.add(taskId);
      dispatched.add(taskId);
    }
    for (const taskId of plan.failed) {
      failed.push(taskId);
      dispatched.add(taskId);
    }

    let snapshot =
      resumed && persisted
        ? persisted
        : initialWorkflowState(workflowId, workflow.definition.tasks, Date.now());

    if (resumed && persisted) {
      // Replay exactly the completions this run had not applied yet.
      this.source.seek(persisted.cursor);
      console.log(
        `[orchestrator] resuming ${workflowId} from cursor ${persisted.cursor} ` +
          `(${plan.completed.length} completed, ${plan.failed.length} failed, ` +
          `${plan.inFlight.length} in flight)`
      );
    } else {
      // A fresh run must not read history. If this workflow id was used before,
      // the completions stream still holds its old outcomes, and replaying them
      // would resolve tasks that have not actually run this time (a stale
      // FAILED would even abort the run before any work happened). Start after
      // everything already published. It also means a fresh run no longer
      // re-scans every other workflow's completions.
      const head = await this.source.headCursor();
      this.source.seek(head);
      if (head !== "0") {
        console.log(
          `[orchestrator] fresh run: ignoring completions at or before ${head}`
        );
      }
    }

    // Persistence is best-effort: a store failure is logged and the run
    // continues — telemetry must never be able to deadlock orchestration.
    const persist = async (): Promise<void> => {
      if (!this.stateStore) return;
      try {
        await this.stateStore.save(snapshot);
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        console.error(
          `[orchestrator] failed to persist workflow state: ${message}`
        );
      }
    };

    // Apply a transition, persist it, and log it.
    const record = async (
      event: WorkflowStateEvent,
      logType: string,
      taskIds: string[],
      detail?: Record<string, unknown>
    ): Promise<void> => {
      snapshot = reduceWorkflowState(snapshot, event);
      await persist();

      if (this.logger) {
        await this.logger.emit({
          ts: new Date(event.at).toISOString(),
          workflowId,
          type: logType,
          taskIds,
          detail,
        });
      }
    };

    // Persist the stream position once a read batch is fully applied, so a
    // restart replays from here rather than re-scanning the whole stream.
    const persistCursor = async (cursor: string): Promise<void> => {
      const advanced = withCursor(snapshot, cursor);
      if (advanced === snapshot) return;
      snapshot = advanced;
      await persist();
    };

    const buildResult = (status: WorkflowRunStatus): WorkflowRunResult => ({
      workflowId,
      status,
      completed: [...completed].sort(),
      failed: [...failed],
      events,
      state: snapshot,
      resumed,
    });

    const finish = async (
      status: WorkflowRunStatus
    ): Promise<WorkflowRunResult> => {
      await record(
        { type: "WORKFLOW_FINISHED", status, at: Date.now() },
        "WORKFLOW_FINISHED",
        [],
        { status }
      );
      return buildResult(status);
    };

    // `getNextExecutableTasks` returns every task whose parents are complete,
    // and `[].every(...)` is vacuously true, so dependency-free roots come back
    // on every call. The `dispatched` filter is what stops double-dispatch.
    const dispatch = async (
      candidates: TaskDefinition[],
      attempt = 1
    ): Promise<void> => {
      const fresh = candidates.filter((t) => !dispatched.has(t.id));
      if (fresh.length === 0) return;

      const ids = await this.sink.dispatchTasks(workflowId, fresh, attempt);
      const taskIds = fresh.map((t) => t.id);
      events.push({ type: "DISPATCHED", taskIds });
      console.log(
        `[orchestrator] dispatched wave [${taskIds.join(", ")}] ` +
          `(attempt ${attempt}, ${ids.length} entries)`
      );

      for (const task of fresh) {
        dispatched.add(task.id);
        await record(
          {
            type: "TASK_DISPATCHED",
            taskId: task.id,
            attempt,
            at: Date.now(),
          },
          "TASK_DISPATCHED",
          [task.id],
          { attempt }
        );
      }
    };

    await record(
      { type: "WORKFLOW_STARTED", at: Date.now() },
      "WORKFLOW_STARTED",
      []
    );

    // Re-dispatch what was in flight when the run stopped, at its persisted
    // attempt. If the task had in fact finished (only the completion was lost),
    // the worker's invocation claim makes this a no-op and the completion
    // arrives from the replayed stream — so nothing runs twice.
    if (resumed) {
      const byAttempt = new Map<number, TaskDefinition[]>();
      for (const { taskId, attempt } of plan.inFlight) {
        const task = tasksById.get(taskId);
        if (!task) continue;
        const group = byAttempt.get(attempt) ?? [];
        group.push(task);
        byAttempt.set(attempt, group);
      }
      for (const [attempt, group] of byAttempt) {
        await dispatch(group, attempt);
      }
    }

    // Covers both the initial wave of a fresh run and any children that were
    // unblocked by seeded completions but never dispatched before the crash.
    await dispatch(resolver.getNextExecutableTasks(completed));

    let lastProgressAt = Date.now();

    for (;;) {
      if (completed.size === total) {
        return await finish("COMPLETED");
      }

      const inFlight = dispatched.size - completed.size - failed.length;

      if (failed.length > 0 && inFlight === 0) {
        return await finish("FAILED");
      }

      if (Date.now() - lastProgressAt >= this.completionTimeoutMs) {
        console.warn(
          `[orchestrator] no progress for ${this.completionTimeoutMs}ms with ` +
            `${inFlight} task(s) in flight — reporting STALLED`
        );
        return await finish("STALLED");
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
          await record(
            { type: "TASK_COMPLETED", completion, at: Date.now() },
            "TASK_COMPLETED",
            [completion.taskId],
            {
              status: completion.status,
              attempt: completion.attempt,
              durationMs: completion.durationMs,
            }
          );

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
          await record(
            { type: "TASK_COMPLETED", completion, at: Date.now() },
            "TASK_FAILED",
            [completion.taskId],
            {
              status: completion.status,
              attempt: completion.attempt,
              exitCode: completion.exitCode,
              timedOut: completion.timedOut,
            }
          );
        }
      }

      // Every entry in the batch has been applied (or deliberately ignored), so
      // the position is now safe to persist.
      await persistCursor(this.source.cursor());
    }
  }

  /**
   * Load a persisted snapshot for this id. Best-effort: a store that cannot be
   * read must not stop the run — it is logged and the run starts fresh.
   */
  private async loadPersisted(
    workflowId: string
  ): Promise<WorkflowStateSnapshot | null> {
    if (!this.stateStore) return null;
    try {
      return await this.stateStore.load(workflowId);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(
        `[orchestrator] failed to load persisted state for ${workflowId}: ` +
          `${message} — starting fresh`
      );
      return null;
    }
  }
}
