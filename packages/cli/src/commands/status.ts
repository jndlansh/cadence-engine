import { RedisWorkflowStateStore } from "@cadence/orchestrator";
import type { TaskRunRecord, WorkflowStateSnapshot } from "@cadence/core";
import { ENV_ERROR, ensureRedis } from "../redis-check.js";

function iso(ms: number): string {
  return new Date(ms).toISOString();
}

function formatTask(id: string, record: TaskRunRecord): string {
  const bits: string[] = [record.status, `attempt ${record.attempt}`];
  if (record.latencyMs !== undefined) bits.push(`${record.latencyMs}ms`);
  if (record.exitCode !== undefined) bits.push(`exit ${record.exitCode}`);
  if (record.timedOut) bits.push("timed out");
  if (record.error) bits.push(`error: ${record.error}`);

  const lines = [`  ${id.padEnd(16)} ${bits.join("  ")}`];

  // A failure is the case worth reading in full, so show what it printed. The
  // capture is already bounded, and every task's output stays available in the
  // state hash (`HGET workflow:<id>:state tasks`) whether it passed or failed.
  if (record.status === "FAILED") {
    for (const [label, text] of [
      ["stdout", record.stdout],
      ["stderr", record.stderr],
    ] as const) {
      if (text) lines.push(`    ${label}: ${text}`);
    }
  }

  return lines.join("\n");
}

/**
 * Render a persisted snapshot as a human-readable report. Pure, so the status
 * output is unit-testable without Redis.
 */
export function formatStatus(snapshot: WorkflowStateSnapshot): string {
  const lines: string[] = [
    `workflow : ${snapshot.workflowId}`,
    `state    : ${snapshot.state}`,
    `started  : ${iso(snapshot.startedAt)}`,
    `updated  : ${iso(snapshot.updatedAt)}`,
  ];

  if (snapshot.completedAt !== undefined) {
    lines.push(`completed: ${iso(snapshot.completedAt)}`);
  }

  lines.push(
    `failed   : ${snapshot.failed.length > 0 ? snapshot.failed.join(", ") : "-"}`
  );
  lines.push("");
  lines.push("TASKS");

  for (const id of Object.keys(snapshot.tasks).sort()) {
    lines.push(formatTask(id, snapshot.tasks[id]));
  }

  return lines.join("\n");
}

/**
 * `cadence status <workflow-id>`: load and print the persisted snapshot.
 * Exit 0 on success, 1 when nothing has been persisted for that id.
 */
export async function statusCommand(
  workflowId: string,
  redisUrl: string
): Promise<number> {
  if (!(await ensureRedis(redisUrl))) {
    return ENV_ERROR;
  }

  const store = new RedisWorkflowStateStore(redisUrl);
  try {
    const snapshot = await store.load(workflowId);
    if (!snapshot) {
      console.error(`[cadence] no persisted state for workflow "${workflowId}"`);
      return 1;
    }
    console.log(formatStatus(snapshot));
    return 0;
  } finally {
    await store.disconnect();
  }
}
