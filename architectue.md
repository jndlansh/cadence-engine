# Cadence Engine — Architecture, Engineering Spec & Agent Guidelines

## 1. Project Mission & System Overview
Cadence is an asynchronous, distributed, fault-tolerant DAG (Directed Acyclic Graph) workflow orchestration engine built end-to-end in TypeScript. It coordinates arbitrary dependency-driven shell and container jobs across decoupled worker processes with rigorous execution guarantees:
- **Strict Topological Invariant:** No child task can execute before all parent dependencies emit clean success exit codes (`0`).
- **Deterministic Cycle Rejection:** Invalid cyclic pipelines are detected and rejected at submission time using Kahn’s Algorithm ($O(V + E)$).
- **At-Least-Once Delivery & Failure Recovery:** Tasks lost to sudden worker crashes (`kill -9`, hardware loss, network partition) are identified via missing heartbeats and claimed from the Redis Pending Entries List (PEL) without dropping jobs.
- **Idempotency Safeguards:** Every task attempt receives a deterministic invocation identifier (`run_<workflow_id>_<task_id>_<attempt>`) to prevent duplicate executions from producing side-effects.

---

## 2. Monorepo Repository Structure
Managed via `pnpm` workspaces. Entries marked `[planned]` are roadmap targets and do **not** exist on disk yet.
```text
cadence/
├── architectue.md                   # System design contract, phases & agent rules (filename is misspelled)
├── docker-compose.yml              # Redis 7 container (Docker path; Memurai used natively on Windows)
├── package.json                    # Workspace root scripts & tooling
├── pnpm-workspace.yaml             # Workspace definitions
├── tsconfig.json                   # Shared TypeScript compiler configuration
├── scripts/
│   ├── smoke-phase3.ts             # Live Redis dispatch + consume smoke test (Phase 3)
│   ├── demo-orchestration.ts       # Live DAG run; asserts the topological invariant (Phase 3.5)
│   ├── demo-failover.ts            # Live worker-kill → reclaim → recovery proof (Phase 4)
│   └── worker-node.ts              # Standalone worker process (fresh child the demo SIGKILLs)
├── workflows/                       # Workflow YAML manifests
│   ├── demo-pipeline.yaml          # 3-wave diamond used by the Phase 3.5 demo
│   ├── failover-pipeline.yaml      # Long-task → dependent, used by the Phase 4 failover demo
│   └── data-pipeline.yaml          # Sample multi-stage DAG workflow   [planned — Phase 6]
└── packages/
    ├── core/                        # Pure domain engine, schemas & process execution
    │   ├── src/types.ts             # Zod contracts & shared interfaces
    │   ├── src/dag.ts               # Kahn's algorithm & dependency traversal
    │   ├── src/dag.test.ts          # Vitest suite for graph invariants
    │   ├── src/parser.ts            # YAML deserialization & runtime validation
    │   ├── src/parser.test.ts       # Vitest suite for YAML loading & edge cases
    │   ├── src/runner.ts            # Subprocess wrapper, stdout/stderr & timeouts
    │   ├── src/runner.test.ts       # Vitest suite for task execution & failure handling
    │   ├── src/streams.ts           # Shared Redis key contract + stream-field folding
    │   ├── src/streams.test.ts      # Vitest suite for the folding helper & key names
    │   └── src/index.ts             # Core package barrel export
    ├── orchestrator/                # Coordination layer & scheduler daemon
    │   ├── src/scheduler.ts         # Redis Streams task dispatcher (XADD)
    │   ├── src/orchestrator.ts      # Runtime DAG walk: wave dispatch + completion handling
    │   ├── src/completions.ts       # Reads cadence:completions via XREAD cursor
    │   ├── src/orchestrator.test.ts # Vitest suite for the DAG walk (faked ports, no Redis)
    │   ├── src/reclaimer.ts         # Stale task reclaimer (XPENDING/XCLAIM + heartbeat gate)
    │   ├── src/reclaimer.test.ts    # Vitest suite for reclaim ordering & the DLQ cap
    │   ├── src/state.ts             # Global workflow run state manager   [planned — Phase 5]
    │   └── src/index.ts             # Orchestrator barrel export
    └── worker/                      # Independent task runner node
        ├── src/consumer.ts          # Redis Streams consumer loop (XREADGROUP) + completion publishing
        ├── src/consumer.test.ts     # Vitest suite for stream parsing & ack ordering
        ├── src/heartbeat.ts         # Periodic liveness beacon (worker:<id>:heartbeat TTL key)
        ├── src/heartbeat.test.ts    # Vitest suite for beat cadence, TTL & failure tolerance
        └── src/index.ts             # Worker barrel export
```

---

## 3. Core Architectural Phases & Roadmap

### Phase 1: Core Domain Engine & Graph Logic `[STATUS: COMPLETED]`

- [x] Defined Zod validation schemas (`TaskSchema`, `WorkflowSchema`) ensuring type-safe boundaries at runtime.
- [x] Implemented Kahn's Algorithm in `DAGResolver` to achieve $O(V + E)$ cycle detection and topological sorting.
- [x] Referential integrity enforcement: workflows fail fast if a task depends on an undefined node ID.
- [x] Dynamic child task evaluation (`getNextExecutableTasks`) based on completed task sets.
- [x] Implemented YAML parsing and validation bridge (`parseWorkflowYAML`).
- [x] Full unit test coverage passing in Vitest.

### Phase 2: Process Execution Layer (`TaskRunner`) `[STATUS: COMPLETED]`

- [x] Child process execution engine using Node.js `child_process.spawn`.
- [x] Cross-platform shell wrapping (`cmd.exe /c` on Windows, `sh -c` on POSIX).
- [x] Process timeout guard: forcefully terminates hung processes (`SIGKILL` on POSIX, `taskkill /T /F` on Windows) after `timeout_ms`.
- [x] Buffered stdout and stderr capture with normalized exit codes (timeout maps to the POSIX convention `124`).
- [x] Execution unit tests validating exit codes, log streaming, and timeout interruptions.

### Phase 3: Distributed State & Queue Infrastructure (Redis Streams) `[STATUS: COMPLETED]`

- [x] Redis client initialization and connection management using `ioredis`.
- [x] Task queuing primitive: publish unlocked tasks to Redis Stream `cadence:tasks` using `XADD`.
- [x] Consumer Group configuration (`XGROUP CREATE`) enabling parallel, load-balanced worker consumption.
- [x] Explicit delivery acknowledgment (`XACK`) once a task exits cleanly.
- [x] Dead Letter Queue (DLQ) routing for tasks exceeding maximum configured retries.

> **Verified** end-to-end against a live Redis (Memurai, native Windows) via `pnpm exec tsx scripts/smoke-phase3.ts`:
> happy path (`COMPLETED` + `XACK`), retry escalation, timeout kill (`exitCode 124`), and DLQ routing.
>
> Scope note: retries and DLQ routing live in the **worker** (`consumer.ts`), not the orchestrator.
> The orchestrator only dispatches and ensures the consumer group. PEL reclamation is deferred to Phase 4.

### Phase 3.5: Runtime DAG Scheduling & Completion Channel `[STATUS: COMPLETED]`

> Added after Phase 3 shipped, because Phase 1's DAG and parser were **orphaned at runtime**:
> `DAGResolver`, `getNextExecutableTasks` and `parseWorkflowYAML` were reachable only from their own unit
> tests, and the strict topological invariant was enforced in a test and nowhere else.

- [x] Completion channel: the worker publishes terminal outcomes (`COMPLETED` | `FAILED`) to the Redis stream `cadence:completions`, **before** `XACK`. Only terminal states are published — the worker retries internally, so the orchestrator never sees intermediate attempts.
- [x] Runtime DAG walk (`WorkflowOrchestrator`): dispatch the initial wave via `getInitialTasks()`, then dispatch newly unblocked children via `getNextExecutableTasks(completed)` as completions arrive.
- [x] A `dispatched` set guards against double-dispatch. `getNextExecutableTasks` returns every task whose parents are complete, and `[].every(...)` is vacuously true, so dependency-free roots are re-returned on every call.
- [x] Fail-fast on terminal task failure: stop dispatching, drain in-flight tasks, report `FAILED`. Descendants are never dispatched.
- [x] Completions are read with a plain `XREAD` and an in-memory cursor, **not** a consumer group — a group would accrue a PEL that nothing drains (see the Phase 4 flag below).
- [x] Idempotent completions: duplicates, foreign `workflowId`s, and completions for never-dispatched tasks are ignored.
- [x] Live verification: `pnpm exec tsx scripts/demo-orchestration.ts` runs a 3-wave diamond (`extract` → `transform_a` + `transform_b` → `load`) and **exits non-zero** if any task is dispatched before all of its parents completed.
- [x] 12 unit tests against faked ports (no Redis required); `pnpm test` → 43 passing.

**`depends_on` deliberately does not cross the wire.** The orchestrator owns the DAG and decides what to
dispatch; the worker is DAG-agnostic, so `buildTaskDefinition()` hardcoding `depends_on: []` is correct.

Known limitation: a malformed task entry cannot emit a completion (there is no `taskId` to attribute it
to), so it is routed to the DLQ only.

---

> ## ✅ THE PHASE 3.5 DEADLOCK IS CLOSED — SEE PHASE 4 BELOW
>
> Phase 3.5 introduced a **new and sharper failure mode**: the orchestrator *waits* for a completion
> before advancing the DAG, so a worker killed mid-task publishes no completion and the workflow
> **deadlocked** (visible only as `STALLED` after `completionTimeoutMs`).
>
> The worker heartbeat + PEL reclaimer below close it. Recovery is now demonstrated live, not merely
> argued: `scripts/demo-failover.ts` SIGKILLs a real worker mid-task and asserts the workflow still
> completes — the task is reclaimed, re-run, and the DAG advances.

### Phase 4: Heartbeats, Task Reclaiming & Worker Fault Tolerance `[STATUS: IN PROGRESS]`

> The deadlock-closing half (heartbeat + reclaimer) is **DONE and live-verified**. The backoff
> scheduler is the remaining item.

- [x] **Closes the Phase 3.5 deadlock (highest priority):** reclaim tasks whose worker died so the orchestrator actually receives a completion.
- [x] Worker Liveness Beacon: workers maintain an ephemeral key with TTL in Redis (`SET worker:<id>:heartbeat "alive" EX 10`) renewed every 3 seconds. Owns its own connection so a blocking `XREADGROUP` cannot starve it.
- [x] Reclaimer Loop (`TaskReclaimer`): background sweeper inspecting the Pending Entries List (PEL) via `XPENDING`.
- [x] Automatic Failover (`XCLAIM`): if a task is un-acked **and** its owner's heartbeat key has expired, the entry is claimed, re-published as a fresh entry (attempt + 1), then acked. Liveness-gated, not idle-only, so a slow-but-alive worker is never stolen from. `XAUTOCLAIM` was declined again.
- [x] Reclaim exhaustion is bounded and never silent: once the retry budget is spent, the task is routed to the DLQ **and** a terminal `FAILED` completion is emitted, so the orchestrator advances instead of deadlocking on a task that can never report back.
- [x] Shared key contract (`packages/core/src/streams.ts`): stream names, the heartbeat key and the flat-field fold live in one place — a mismatched key fails silently, which is this project's recurring bug class.
- [x] 16 new unit tests (`pnpm test` → 59 passing: 27 core + 13 worker + 19 orchestrator).
- [ ] Exponential backoff scheduler for failed tasks using Redis Sorted Sets (`ZSET`) — **REMAINING.**

### Phase 5: Workflow State Machine & Lifecycle Persistence `[STATUS: NOT STARTED]`

- [ ] Workflow lifecycle state management: `PENDING` $\to$ `RUNNING` $\to$ `COMPLETED` | `FAILED`.
- [ ] Tracking task execution attempts, execution latency, and error states.
- [ ] Structured file/console event logger.

### Phase 6: Orchestrator CLI & Workflow Submission `[STATUS: NOT STARTED]`

- [ ] CLI runner to trigger workflows: `pnpm cadence run ./workflows/data-pipeline.yaml`.
- [ ] Live terminal progress bar rendering active, completed, and failed tasks in real-time.
- [ ] Multi-worker launch script using Docker or local concurrent processes for demo purposes.

---

## 4. Operating Rules for AI Coding Agents

Any AI assistant modifying or contributing to this repository **MUST** strictly follow these rules:

1. **Strict TypeScript & Single Source of Truth**
   - Never use `any`. Always use explicit, strict typing.
   - All shared schemas and types must live in `packages/core/src/types.ts`.
   - Never import queue, database, or network modules into `packages/core`. The core package must stay completely pure and deterministic.

2. **Failure-First Distributed System Assumption**
   - Assume every worker can crash at any millisecond.
   - Never call `XACK` before a command has fully exited and its state is durably recorded.
   - All tasks must have a timeout and bounded retry count.

3. **Cross-Platform Compatibility**
   - Do not assume a Linux environment. The code must run seamlessly across Windows (`cmd.exe`/PowerShell) and POSIX (`sh`/`bash`).
   - Use Node.js standard library path modules (`node:path`, `node:child_process`) rather than hardcoded slash paths.
   - On Windows, Redis (Memurai) binds IPv4 only. Prefer `redis://127.0.0.1:6379` over `localhost`, since Node >= 18 resolves `localhost` verbatim and may attempt `::1` first.

4. **Mandatory Test-Driven Quality**
   - Every algorithmic change, edge-case fix, or utility function must include a unit test in a corresponding `*.test.ts` file.
   - All changes must pass `pnpm test` with zero failures before being considered complete.

5. **Live Verification for Queue & Stream Code**
   - Unit tests are necessary but never sufficient for Redis Streams code. Any change to `scheduler.ts` or `consumer.ts` must be exercised against a running Redis with `scripts/smoke-phase3.ts`.
   - Redis returns stream entry fields as a **flat `[field, value, field, value, ...]` array**, not an object. Never index entry fields by key without folding them first — doing so silently discards every task.
   - Terminal task outcomes are published to the `cadence:completions` stream. **Publish a completion before `XACK`**, exactly as with re-dispatch and DLQ routing: acknowledging first creates an unrecoverable window.
   - Never discard anything silently. A dropped task must be logged and/or routed to the DLQ; two separate bugs hid behind a silent discard.
   - Before accepting a regression test, confirm it actually **fails** against the pre-fix code.
