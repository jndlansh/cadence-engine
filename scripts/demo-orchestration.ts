/**
 * Phase 3.5 live demo: workflow YAML -> DAG -> Redis Streams -> worker ->
 * completions -> next wave.
 *
 * Run with:  pnpm exec tsx scripts/demo-orchestration.ts
 *
 * Unlike scripts/smoke-phase3.ts this driver ASSERTS the topological invariant
 * and exits non-zero when it is violated, so it is a real gate rather than an
 * observation. It pins 127.0.0.1 for the same reason the smoke test does: the
 * Windows Redis binds IPv4 only.
 *
 * Clear the streams first:
 *   memurai-cli DEL cadence:tasks cadence:tasks:dlq cadence:completions
 */
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import {
  parseWorkflowYAML,
  type ParsedWorkflow,
} from "../packages/core/src/parser.js";
import { TaskDispatcher } from "../packages/orchestrator/src/scheduler.js";
import {
  WorkflowOrchestrator,
  type OrchestrationEvent,
} from "../packages/orchestrator/src/orchestrator.js";
import { RedisCompletionSource } from "../packages/orchestrator/src/completions.js";
import { WorkerConsumer } from "../packages/worker/src/consumer.js";

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";
const WORKFLOW_ID = "demo_wf_1";
const COMPLETION_TIMEOUT_MS = 20_000;

// The root package is CommonJS (no "type": "module"), so `import.meta` is not
// available in this directory. Resolve against the working directory instead,
// which is the repo root when run via `pnpm exec tsx scripts/demo-orchestration.ts`.
// Override with a path argument if needed.
const WORKFLOW_FILE =
  process.argv[2] ?? join(process.cwd(), "workflows", "demo-pipeline.yaml");

/**
 * Replay the event log and confirm no task was ever dispatched before all of
 * its parents had completed. This is the strict topological invariant from the
 * architecture doc, checked against what actually happened.
 */
function findTopologyViolations(
  workflow: ParsedWorkflow,
  events: OrchestrationEvent[]
): string[] {
  const completedSoFar = new Set<string>();
  const violations: string[] = [];

  for (const event of events) {
    for (const taskId of event.taskIds) {
      if (event.type === "COMPLETED") {
        completedSoFar.add(taskId);
        continue;
      }

      if (event.type !== "DISPATCHED") continue;

      const task = workflow.definition.tasks.find((t) => t.id === taskId);
      if (!task) continue;

      for (const parentId of task.depends_on) {
        if (!completedSoFar.has(parentId)) {
          violations.push(
            `${taskId} was dispatched before parent ${parentId} completed`
          );
        }
      }
    }
  }

  return violations;
}

async function main(): Promise<void> {
  const raw = await readFile(WORKFLOW_FILE, "utf-8");
  const workflow = parseWorkflowYAML(raw);

  console.log(
    `[demo] workflow=${workflow.definition.workflow} tasks=${workflow.definition.tasks.length}`
  );
  console.log(
    `[demo] initial wave: ${workflow.resolver
      .getInitialTasks()
      .map((t) => t.id)
      .join(", ")}`
  );

  const dispatcher = new TaskDispatcher(REDIS_URL);
  const completions = new RedisCompletionSource(REDIS_URL);
  const worker = new WorkerConsumer(REDIS_URL);

  await dispatcher.ensureGroup();
  console.log(`[demo] group ensured; worker=${worker.getWorkerId()}`);

  const workerLoop = worker.start();

  const orchestrator = new WorkflowOrchestrator(dispatcher, completions, {
    completionTimeoutMs: COMPLETION_TIMEOUT_MS,
  });

  const result = await orchestrator.run(WORKFLOW_ID, workflow);

  worker.requestShutdown();
  await workerLoop;
  await worker.disconnect();
  await completions.disconnect();
  await dispatcher.disconnect();

  console.log("\n[demo] event log:");
  for (const event of result.events) {
    console.log(`  ${event.type.padEnd(10)} ${event.taskIds.join(", ")}`);
  }

  console.log(
    `\n[demo] status=${result.status} completed=${result.completed.length}/${workflow.definition.tasks.length} failed=[${result.failed.join(", ")}]`
  );

  const violations = findTopologyViolations(workflow, result.events);
  if (violations.length > 0) {
    console.error("\n[demo] TOPOLOGICAL INVARIANT VIOLATED:");
    for (const violation of violations) {
      console.error(`  - ${violation}`);
    }
    process.exit(1);
  }
  console.log(
    "[demo] topological invariant holds: no task ran before its parents completed"
  );

  if (result.status !== "COMPLETED") {
    console.error(`[demo] expected status COMPLETED but got ${result.status}`);
    process.exit(1);
  }

  console.log("[demo] done - now inspect with memurai-cli");
  process.exit(0);
}

main().catch((err: unknown) => {
  console.error("[demo] fatal:", err);
  process.exit(1);
});
