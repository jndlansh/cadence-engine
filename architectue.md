# Cadence Engine — Architecture, Engineering Spec & Agent Guidelines

## 1. Project Mission & System Overview
Cadence is an asynchronous, distributed, fault-tolerant DAG (Directed Acyclic Graph) workflow orchestration engine built end-to-end in TypeScript. It coordinates arbitrary dependency-driven shell and container jobs across decoupled worker processes with rigorous execution guarantees:
- **Strict Topological Invariant:** No child task can execute before all parent dependencies emit clean success exit codes (`0`).
- **Deterministic Cycle Rejection:** Invalid cyclic pipelines are detected and rejected at submission time using Kahn’s Algorithm ($O(V + E)$).
- **At-Least-Once Delivery & Failure Recovery:** Tasks lost to sudden worker crashes (`kill -9`, hardware loss, network partition) are identified via missing heartbeats and claimed from the Redis Pending Entries List (PEL) without dropping jobs.
- **Idempotency Safeguards:** Every task attempt receives a deterministic invocation identifier (`run_<workflow_id>_<task_id>_<attempt>`) to prevent duplicate executions from producing side-effects. Enforced by the worker's atomic claim (`SET invocation:<id> … NX`) before the command runs — see the Idempotency Guard section in §3.

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
│   ├── demo-state.ts               # Live persisted lifecycle proof: COMPLETED + FAILED (Phase 5)
│   └── worker-node.ts              # Standalone worker process (fresh child the demo SIGKILLs)
├── workflows/                       # Workflow YAML manifests
│   ├── demo-pipeline.yaml          # 3-wave diamond used by the Phase 3.5 demo
│   ├── failover-pipeline.yaml      # Long-task → dependent, used by the Phase 4 failover demo
│   └── data-pipeline.yaml          # Multi-stage DAG used by the Phase 6 CLI demo
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
    │   ├── src/retry.ts             # Pure exponential-backoff math + retry payload codec
    │   ├── src/retry.test.ts        # Vitest suite for backoff delays & payload encoding
    │   ├── src/state.ts             # Pure workflow lifecycle state machine (reducer)
    │   ├── src/state.test.ts        # Vitest suite for lifecycle transitions
    │   ├── src/invocation.ts        # Pure idempotency contract: invocation id, claim key, decision
    │   ├── src/invocation.test.ts   # Vitest suite for id building, key naming & claim decisions
    │   └── src/index.ts             # Core package barrel export
    ├── orchestrator/                # Coordination layer & scheduler daemon
    │   ├── src/scheduler.ts         # Redis Streams task dispatcher (XADD)
    │   ├── src/orchestrator.ts      # Runtime DAG walk: wave dispatch + completion handling
    │   ├── src/completions.ts       # Reads cadence:completions via XREAD cursor
    │   ├── src/orchestrator.test.ts # Vitest suite for the DAG walk (faked ports, no Redis)
    │   ├── src/reclaimer.ts         # Stale task reclaimer (XPENDING/XCLAIM + heartbeat gate)
    │   ├── src/reclaimer.test.ts    # Vitest suite for reclaim ordering & the DLQ cap
    │   ├── src/promoter.ts          # Retry promoter (ZRANGEBYSCORE due -> XADD -> ZREM)
    │   ├── src/promoter.test.ts     # Vitest suite for promotion ordering & the DLQ path
    │   ├── src/state-store.ts       # RedisWorkflowStateStore: persists the lifecycle snapshot
    │   ├── src/state-store.test.ts  # Vitest suite for save/load round-trip & malformed state
    │   ├── src/event-logger.ts      # WorkflowEventLogger: console + JSON-lines telemetry
    │   ├── src/event-logger.test.ts # Vitest suite for the structured event log
    │   └── src/index.ts             # Orchestrator barrel export
    ├── worker/                      # Independent task runner node
    │   ├── src/consumer.ts          # Redis Streams consumer loop (XREADGROUP) + completion publishing
    │   ├── src/consumer.test.ts     # Vitest suite for stream parsing & ack ordering
    │   ├── src/heartbeat.ts         # Periodic liveness beacon (worker:<id>:heartbeat TTL key)
    │   ├── src/heartbeat.test.ts    # Vitest suite for beat cadence, TTL & failure tolerance
    │   └── src/index.ts             # Worker barrel export
    └── cli/                         # Orchestrator CLI (Phase 6)
        ├── src/main.ts              # Entry: exit-code mapping, SIGINT handling
        ├── src/program.ts           # commander program; run/status subcommands
        ├── src/progress.ts          # buildProgressView + ProgressStateStore decorator + reporters
        ├── src/workers.ts           # Spawns N local worker children, waits for WORKER_READY
        ├── src/commands/run.ts      # `run <file>`: dispatch, workers, orchestrate, render
        ├── src/commands/status.ts   # `status <id>`: print the persisted lifecycle snapshot
        └── src/index.ts             # CLI barrel export
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

### Phase 4: Heartbeats, Task Reclaiming & Worker Fault Tolerance `[STATUS: COMPLETED]`

> Both halves are **DONE and live-verified**: the deadlock-closing heartbeat + reclaimer, and the
> exponential-backoff retry scheduler.

- [x] **Closes the Phase 3.5 deadlock (highest priority):** reclaim tasks whose worker died so the orchestrator actually receives a completion.
- [x] Worker Liveness Beacon: workers maintain an ephemeral key with TTL in Redis (`SET worker:<id>:heartbeat "alive" EX 10`) renewed every 3 seconds. Owns its own connection so a blocking `XREADGROUP` cannot starve it.
- [x] Reclaimer Loop (`TaskReclaimer`): background sweeper inspecting the Pending Entries List (PEL) via `XPENDING`.
- [x] Automatic Failover (`XCLAIM`): if a task is un-acked **and** its owner's heartbeat key has expired, the entry is claimed, re-published as a fresh entry (attempt + 1), then acked. Liveness-gated, not idle-only, so a slow-but-alive worker is never stolen from. `XAUTOCLAIM` was declined again.
- [x] Reclaim exhaustion is bounded and never silent: once the retry budget is spent, the task is routed to the DLQ **and** a terminal `FAILED` completion is emitted, so the orchestrator advances instead of deadlocking on a task that can never report back.
- [x] Shared key contract (`packages/core/src/streams.ts`): stream names, the heartbeat key and the flat-field fold live in one place — a mismatched key fails silently, which is this project's recurring bug class.
- [x] 16 new unit tests for the failover half (`pnpm test` → 59 passing at the time: 27 core + 13 worker + 19 orchestrator).
- [x] Exponential backoff scheduler for failed tasks using Redis Sorted Sets (`ZSET`): the worker `ZADD`s a failed task into `cadence:retries` scored by its due time (epoch ms) instead of re-publishing immediately, then acks its entry. The orchestrator's `RetryPromoter` sweeps due members (`ZRANGEBYSCORE -inf now LIMIT`), re-`XADD`s each onto `cadence:tasks`, then `ZREM`s it — publish-before-remove, mirroring publish-before-ack.
  - Backoff math is pure and lives in `packages/core/src/retry.ts` (`computeBackoffDelayMs`, `encodeRetryPayload`/`decodeRetryPayload`), so the delay curve and the member encoding cannot drift; the cap is applied before the exponent can overflow.
  - The retry member encodes the whole payload, so the promoter needs no other state; each attempt is a distinct member, so re-scheduling the same attempt is an idempotent score update.
  - Never silent: a malformed member is routed to the DLQ (`reason=RETRY_MALFORMED`) and removed; a failed promote leaves the member in place for the next sweep.
  - The reclaimer still re-dispatches a *reclaimed* task immediately (a worker death is not a task failure) — backoff applies only to tasks that actually failed and still have retry budget.
  - Test totals: `pnpm test` → **75 passing** (38 core + 13 worker + 24 orchestrator). Promoter wired into `scripts/smoke-phase3.ts`, `scripts/demo-orchestration.ts`, and `scripts/demo-failover.ts`.

### Phase 5: Workflow State Machine & Lifecycle Persistence `[STATUS: COMPLETED]`

- [x] Workflow lifecycle state management: `PENDING` $\to$ `RUNNING` $\to$ `COMPLETED` | `FAILED` | `STALLED` (the terminal `STALLED` mirrors the orchestrator's stall guard).
- [x] Pure reducer (`packages/core/src/state.ts`): `initialWorkflowState`, `reduceWorkflowState`, `isTerminalWorkflowState`. No I/O, so the lifecycle rules are deterministic and unit-testable in isolation; transitions are monotonic and idempotent (a duplicate completion or any event after a terminal state is ignored).
- [x] Task execution tracking: a per-task record carrying status, attempt, `dispatchedAt`, `completedAt`, execution `durationMs`, end-to-end `latencyMs` (dispatch $\to$ completion, including queue wait), `exitCode`, `timedOut` and `error`.
- [x] Persistence (`orchestrator/src/state-store.ts`): `RedisWorkflowStateStore` writes each snapshot to the Redis hash `workflow:<id>:state` (`workflowStateKey()` in `core/src/streams.ts`). It survives an orchestrator restart, `HSET` overwrites atomically, and it is inspectable with `HGETALL` / `HGET ... tasks`. A malformed or invalid persisted hash is logged and returns `null` — never silently.
- [x] Structured event logger (`orchestrator/src/event-logger.ts`): `WorkflowEventLogger` emits a compact console line plus an optional JSON-lines file. It is injected through a `WorkflowEventSink` port, so the DAG walk stays testable with a fake.
- [x] The completion payload now carries optional **`timedOut`** and **`error`** fields, so failure states record real detail instead of inferring from `exitCode`. `timedOut` is decoded explicitly from the stream string — **not** via `z.coerce.boolean()`, which would read `"false"` as `true`.
- [x] Persistence is **best-effort**: a `stateStore` write failure is logged and the run continues. Telemetry must never be able to deadlock orchestration.
- [x] Live verification: `pnpm exec tsx scripts/demo-state.ts` — run 1 (the diamond) ends `COMPLETED` and the snapshot is read back from Redis and asserted (attempt/latency present for every task); run 2 (a failing pipeline) ends `FAILED`, with `never` never dispatched and the persisted `failed` list naming `boom`. The script exits non-zero on any violation.
- [x] 29 new unit tests (core `state` 10, core `streams` +1, orchestrator `state-store` 6, `event-logger` 4, `orchestrator` +7, worker `consumer` +1); `pnpm test` → **104 passing** (48 core + 14 worker + 42 orchestrator).

**Decision (was open, now closed):** state is persisted in a Redis hash per workflow, not in-memory only — consistent with the at-least-once / crash-survival posture of the rest of the engine.

### Phase 6: Orchestrator CLI & Workflow Submission `[STATUS: COMPLETED]`

- [x] CLI runner: `pnpm cadence run ./workflows/data-pipeline.yaml` (root `cadence` script → `tsx packages/cli/src/main.ts`). Built with **commander**; `run` and `status` subcommands.
- [x] Live progress bar (via **cli-progress**): a `ProgressStateStore` decorator over the Redis state store repaints on every lifecycle save, so progress reflects the persisted truth. Plain one-line output when not a TTY or with `--no-progress`; the orchestrator's own console output is bridged above the bar so it cannot tear it.
- [x] `run` **spawns N local worker children** (`--workers N`) via `scripts/worker-node.ts`, waits for `WORKER_READY`, and stops them on completion (Docker Compose remains the container path).
- [x] `status <workflow-id>` prints the persisted snapshot (state, timestamps, per-task attempt/latency/exit/error) — the Phase 5 hash's first consumer.
- [x] Exit codes: **0** COMPLETED, **1** FAILED / runtime error, **2** usage / parse error, **3** STALLED, **4** environment (Redis unreachable).
- [x] **Redis preflight**: both commands `ping` Redis before creating any client, worker, or stream, so an unreachable server produces one actionable message ("cannot connect to Redis at …; start Memurai or pass --redis-url") and exit 4, instead of an ioredis retry storm. `pingRedis` lives in `orchestrator/src/connectivity.ts`. Subcommands also inherit commander's `exitOverride`, so a bad invocation exits 2 rather than 1.
- [x] `workflows/data-pipeline.yaml` created (6-task multi-stage DAG).
- [x] **Additive**: no changes to `core` or `worker`; the CLI reuses the Phase 5 `WorkflowStateStore` port and a store decorator. The only orchestrator change is a new `connectivity.ts` (`pingRedis`) exported from its barrel.
- [x] 19 new unit tests (CLI `program` 5, `progress` 4, `workers` 4, `status` 3, `redis-check` 3); `pnpm test` → **123 passing** (48 core + 14 worker + 42 orchestrator + 19 CLI).

**Live (verified 2026-11-01 via a throwaway `memurai.exe` on 127.0.0.1:6379, stopped afterwards):**
`pnpm cadence run workflows/data-pipeline.yaml --workers 2` → live progress through the 6-task DAG → `COMPLETED`, exit 0. `pnpm cadence status <id>` prints the persisted table. A failing workflow exits 1 (fail-fast held: the child never ran); a missing id exits 1; an unknown command exits 2; **with Redis stopped, `run`/`status` print one actionable message and exit 4** (no ioredis retry storm), and a missing `status` argument exits 2.

### Idempotency Guard (post-roadmap item 2a) `[STATUS: COMPLETED]`

> The mission has always *claimed* idempotency via `run_<workflow_id>_<task_id>_<attempt>`, but nothing
> consumed that id: because delivery is at-least-once, a redelivery re-ran a real command. This closes
> the gap between the documented guarantee and the code.

- [x] **Pure contract** (`core/src/invocation.ts`): `buildInvocationId` — the single place the id is
  formatted (it was duplicated inline in `runner.ts` and `consumer.ts` before), `invocationKey`
  (`invocation:<id>`), `INVOCATION_TTL_SECONDS` (default 86 400), and `decideInvocationClaim`, which
  pins ioredis's `SET ... NX` reply convention (`"OK"` = acquire, `null` = duplicate) in one tested place.
- [x] **The worker claims before executing** (`consumer.ts`): `SET invocation:<id> <workerId> EX <ttl> NX`
  runs immediately before `TaskRunner.run()`. The first delivery wins; a later delivery of the same
  invocation logs a duplicate warning, **does not execute the command**, and acks its entry. Claiming
  *before* the command is deliberate: a crash after the claim leaves the entry pending and recoverable,
  whereas claiming after would let a duplicate repeat a side effect that already happened.
- [x] **Fail-closed**: if the claim itself fails, the entry is not acked — it stays pending and is
  recovered, rather than running unguarded.
- [x] **Scoped per attempt, not per task**: a genuine retry (attempt 2) has a different invocation id and
  runs normally, so the retry budget is unaffected.
- [x] `INVOCATION_TTL_SECONDS` wired through `scripts/worker-node.ts` for the spawned-worker path.
- [x] 9 new unit tests (core `invocation` 6, worker `consumer` +3). One is a true regression: it fails
  against the pre-guard code with "expected run to be called 1 times, but got 2 times". `pnpm test` → **132 passing**.
- [x] **Live-verified** (2026-11-16, throwaway `memurai.exe` on `127.0.0.1:6379`): a clean
  `scripts/smoke-phase3.ts` run executed each of the 3 tasks once (XPENDING 0, DLQ 2, ZCARD 0, one
  `invocation:*` key per execution); re-running it with the claim keys intact suppressed all three
  (`duplicate delivery of run_… — already claimed, skipping execution`); clearing `invocation:*` restored
  normal execution. `pnpm cadence run workflows/data-pipeline.yaml --workers 2` → `COMPLETED` 6/6, 6 claim keys.

**Operational consequence (deliberate).** The invocation id is deterministic, so re-running a workflow
*with the same workflow id* is the same invocation and is suppressed for the claim TTL. The smoke and
demo scripts use fixed ids (`smoke_wf_1`, `demo_wf_1`, `demo_wf_1`/`failed_wf_1`, `failover_wf_1`), so
their cleanup recipes must now also clear `invocation:*`:

```bash
memurai-cli EVAL "local k=redis.call('keys',ARGV[1]) for i=1,#k do redis.call('del',k[i]) end return #k" 0 "invocation:*"
```

The CLI sidesteps this by defaulting to a unique id (`<workflow>_<timestamp>`).

**Residual window — stated plainly.** Idempotency is keyed on the *attempt*, so a task whose worker
**died** is re-published by the reclaimer as **attempt + 1** — a new invocation — and may still run the
command a second time. Closing that fully means either task-level (not attempt-level) dedup, or
restart-resume reconciling `workflow:<id>:state` before re-dispatch — which is the next item, now done.

### Restart-Resume (post-roadmap item 2b) `[STATUS: COMPLETED]`

> Closes the last failure mode of Phase 5: a persisted `RUNNING` workflow used to be orphaned forever when
> the orchestrator restarted. The run now continues exactly where it stopped.

- [x] **The completions cursor is persisted** — `cursor` on `WorkflowStateSnapshot`, written to
  `workflow:<id>:state`. The orchestrator advances it only after a read batch has been *applied*, so a
  crash mid-batch replays that batch (harmless — completions are idempotent) instead of skipping it.
  `CompletionSource` gained `cursor()` / `seek()`; `withCursor()` in core is the only way it moves.
- [x] **`planWorkflowResume`** (pure, `core/src/state.ts`) reads a snapshot and returns `FRESH` (nothing
  persisted, or the run already terminal — start over) or `RESUME` with the task records split into
  `completed`, `failed` and `inFlight`.
- [x] **`WorkflowOrchestrator.run(id, workflow, { resume })`**: a non-terminal snapshot is **always**
  resumed (orphaning it is never right); a terminal one is returned as-is only when `resume: true`,
  otherwise the run starts fresh with a warning. `WorkflowRunResult` gained `resumed`.
- [x] **In-flight tasks are re-dispatched at their persisted attempt, never `attempt + 1`.** That is what
  makes resume idempotent: if the task had actually finished and only its completion was lost, the
  worker's invocation claim suppresses the re-dispatch and the completion is replayed from the cursor —
  the command never runs twice. (A task whose *worker died* therefore stays suppressed at that attempt;
  recovering it remains the reclaimer's job.)
- [x] **A latent gap closed for free:** the initial wave is now dispatched from
  `getNextExecutableTasks(completed)` rather than `getInitialTasks()`, so on resume a child that was
  unblocked but never dispatched (crash in between) is still picked up.
- [x] **CLI**: `run --resume` — requires an explicit `--workflow-id` (usage error, exit 2); an id with no
  persisted state reports one clear line and exits 1.
- [x] 20 new unit tests (core `state` +9, orchestrator +9, state-store +1, CLI program +1) → `pnpm test`
  **152 passing**. The 8 resume tests were confirmed to **fail** against pre-resume code (without it a
  resumed run restarts from the first task and dies at the stall guard).
- [x] **Live-verified (2026-11-16, throwaway `memurai.exe`, stopped afterwards):** a fabricated
  "orchestrator crashed" world — `extract` COMPLETED, `validate` in flight with its completion sitting
  unapplied in the stream and its dead worker's claim still set — resumed to `COMPLETED`. `validate` was
  re-dispatched at attempt 1 and the worker **suppressed** it (`duplicate delivery … skipping
  execution`); `extract` was never re-dispatched (and has no invocation claim, so it provably never ran
  twice). Fresh runs are unaffected, and their `cursor` is now persisted in the state hash.

**Found live — worth remembering.** Renaming `RedisCompletionSource`'s private `cursor` field to
`position` left `read()` passing `this.cursor` to `XREAD`. That reference silently became the **method**,
so ioredis received a function as its final argument, treated it as a node-style callback, and crashed
with `Cannot read properties of undefined (reading 'position')`. Neither `tsc` nor any unit test could
see it — both are legal in that argument position, and `RedisCompletionSource` isn't unit-tested (the
orchestrator tests inject a fake). Only the live run caught it. The field is now `cursorPosition`.

**Resolved 2026-11-16 (see the section below):** the hazard noted here — a *fresh* run that reuses an
explicit id reading its completions from `0` and replaying that id's old outcomes — is fixed by starting a
fresh run at the stream head.

### Fresh-Run Completions Cursor (step 1, item 4) `[STATUS: COMPLETED]`

> A fresh run used to read the completions stream from position `0`. If that workflow id had been used
> before, the stream still held the **earlier run's** outcomes — and they were applied to the new run.

- [x] **`CompletionSource.headCursor()`** — the position of the newest entry currently in the stream
  (`"0"` when empty), read with `XREVRANGE … COUNT 1`. (`XREVRANGE`, not `XINFO STREAM`: a missing key is
  an empty reply rather than an error.)
- [x] **A fresh run seeks to the head before dispatching anything**, so it can only ever observe
  completions published after it started. Resume is unchanged — it seeks to the persisted cursor instead.
- [x] **Why it mattered (both symptoms are silent wrongness** — the result would not describe the work the
  run actually did): a stale `FAILED` made a fresh run **abort before doing any work** (fail-fast on a
  completion for a task that had not run), and a stale `COMPLETED` made it **advance — or report success —
  without executing the task**.
- [x] **Side benefit:** a fresh run no longer re-scans every other workflow's completions on each read.
- [x] 3 new unit tests; the two that matter were confirmed to **fail** pre-fix (`expected [ '0' ] to deeply
  equal [ '2' ]` and `expected 'FAILED' to be 'COMPLETED'`). `pnpm test` → **155 passing**.
- [x] **Live-verified (2026-11-16, throwaway `memurai.exe`, stopped afterwards):** with a stale `FAILED`
  seeded for `stale_wf_1`/`extract`, a fresh `--workflow-id stale_wf_1` logged
  `fresh run: ignoring completions at or before 1794829204006-0`, executed all six tasks and reported
  `COMPLETED` — and the stale entry was still in the stream afterwards (XLEN 7 = 1 stale + 6 real),
  proving it was skipped rather than consumed.

**Known trap this exposes (open, logged in step 1 below).** Re-running an id that has already finished
cannot re-run its work: the attempts are claimed (item 2a), so the workers correctly suppress them and the
run ends `STALLED`. That is honest — pre-fix it silently "completed" — but a fresh `run` of a finished id
should refuse explicitly rather than warn and stall.

### Cross-Attempt Duplicate Window (step 1, item 1) `[STATUS: COMPLETED]`

> The idempotency claim is per **attempt**, so a task re-published by the reclaimer as `attempt + 1`
> carried a *new* invocation id that no claim could suppress. A worker that died between finishing its
> command and acking the entry therefore had its work executed a second time.

- [x] **The worker records a terminal outcome before routing it**, under `invocation:<invocationId>:outcome`
  (`core/src/invocation.ts`: `InvocationOutcomeSchema`, `encodeInvocationOutcome` /
  `decodeInvocationOutcome`, `invocationOutcomeKey`). Best-effort: a failed write still lets the run
  complete, because the record only matters if the process dies before the ack.
- [x] **Only terminal outcomes are recorded.** A failure that still has retry budget is deliberately
  absent — replaying a `FAILED` completion for an attempt that is about to be retried would abort the
  workflow. `hasRetryBudget()` is now the single definition shared by the worker's retry branch and the
  reclaimer's budget, so the two cannot drift apart.
- [x] **The reclaimer resolves from the record before re-publishing.** If the dead attempt has a recorded
  outcome, that outcome is replayed as a completion (plus, for a terminal failure, the DLQ record the
  worker never got to write) and the entry is acked — **the command is never re-dispatched**. A record
  that exists but cannot be parsed is logged and falls back to re-dispatching: never a silent discard.
- [x] `SweepResult` / `getStats()` gained `replayed`, so a replay is visible instead of being folded into
  "reclaimed".
- [x] 16 new unit tests (core `invocation` +9, worker `consumer` +4, reclaimer +3) → `pnpm test`
  **171 passing**. Eight were confirmed to **fail** pre-fix, including `replayed` coming back empty —
  i.e. the reclaimer re-dispatching instead of replaying.
- [x] **Live-verified (2026-11-16, throwaway `memurai.exe`, stopped afterwards):**
  - *Regression on the load-bearing path:* `scripts/demo-failover.ts` still reports `reclaimed=1 DLQ=0`
    and recovers — a worker SIGKILLed **mid-command** records no outcome, so it is correctly re-dispatched
    as attempt 2.
  - *New behaviour:* a task entry stranded under a dead consumer, with a recorded `COMPLETED` outcome,
    swept to `examined=1 reclaimed=1 replayed=1` — and `XLEN cadence:tasks` stayed at **1** (no
    re-dispatch), `XPENDING` fell to 0, and `cadence:completions` received the replayed outcome
    (`attempt 1`, `exitCode 0`, `durationMs 42` preserved).

**What is still exposed, and why it is inherent.** A worker killed *while the command is running* has no
record, so its task is re-dispatched and the command runs again — at-least-once, by design. Closing that
needs the *task* to be idempotent, and the natural lever is the invocation id, which the runner could
expose to the command as an env var (`CADENCE_INVOCATION_ID`). Logged for step 4 (per-task `env`).

### The next six steps (logged 2026-11-16 for future reference — none of them started)

Ordered so each step makes the next one safe. **Step 1 is next.**

1. **Close the remaining correctness holes.**
   - ✅ **DONE 2026-11-16 — cross-attempt duplicate window** (see the section above): the worker records a
     terminal outcome and the reclaimer replays it instead of re-running the command.
   - Persist bounded task stdout/stderr, so a failed run is debuggable after the fact (today output only
     survives on the DLQ/completion).
   - Workflow-level timeout and `cadence cancel <id>`, propagated to in-flight children.
   - Expose the invocation id to the task itself (e.g. `CADENCE_INVOCATION_ID`), so a command that *must*
     be idempotent has the lever to be — the mission promises this and nothing currently provides it.
   - ✅ **DONE 2026-11-16 — fresh-run cursor** (see the section above): a fresh run now starts reading
     completions at the stream head instead of replaying the id's history.
   - **Decide what a fresh `run` of an already-finished id should do.** Today it warns and starts, and
     because the attempts are already claimed the tasks are suppressed, so the run honestly reports
     `STALLED` (verified live). Truthful, but poor UX — either refuse it up front with a clear error, or
     treat it as an implicit `--resume`.
2. **Hardening and documented debt.** `XADD … MAXLEN ~` trimming — carefully: trimming a stream that
   still has un-acked PEL entries drops work *silently*, so trim below the group's last-delivered id.
   Revert the redundant `redis.call("XREADGROUP", …)`. Promote the smoke/demo scripts into Redis-gated
   integration tests wired into `pnpm test`. Worker concurrency (`COUNT 1` → N) with real backpressure.
   Graceful shutdown that drains in-flight work. A levelled logger instead of bare `console.*`. CI:
   typecheck + tests + a Redis service container.
3. **Operability.** Dockerfiles and a compose file that runs the **orchestrator as a service plus N worker
   replicas** (today `docker-compose.yml` is Redis only). Health and metrics endpoints; Prometheus metrics
   derived from the state hash; aggregated logs. `cadence dlq list|replay|purge`, `cadence ps|logs`, and a
   TUI/web dashboard over `workflow:*:state`.
4. **Richer DAG semantics.** Per-task `env`/`cwd`/secrets and resource limits. Conditional/branching
   edges, fan-out `map`, per-task retry policy with jitter, cleanup/`always` steps. Sub-workflows and
   workflow chaining (A completes → start B).
5. **Service surface — and the trust boundary.** An HTTP submit endpoint, webhooks, and cron scheduling
   (reusing the Phase 4 ZSET timing primitives). **Container execution** (`docker run`): the mission
   promises "shell/container jobs", and running arbitrary shell is remote code execution by design, so a
   container is the isolation boundary. Then multi-tenancy: namespaces, quotas, authn/authz on submission,
   secret management.
6. **Packaging and release.** Publish the packages with semver, `CHANGELOG` and `LICENSE` (or ship a
   single `cadence` entrypoint / images). A config file with a **validated env schema** — the implicit env
   contract is already growing (`REDIS_URL`, `HEARTBEAT_*`, `RETRY_*`, `INVOCATION_TTL_SECONDS`). Docs:
   README quickstart, architecture, operations runbook, and a **tracked** context/handoff doc, since
   `.freebuff/` is git-ignored and does not travel with the repo. Add `.gitignore` (`dump.rdb`,
   `.commandcode/`, `.freebuff/`). Version the persisted state schema.

**Dependencies:** 1 → 2 → 3 → 4, with 5 and 6 in parallel once a single instance is trustworthy. There is
no point exposing a service (5) or building a dashboard (3) on top of an engine that can still double-run
work (1) or strand a run on restart (2b — now done).

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
   - Every task attempt is claimed with `SET invocation:<invocationId> … NX` (in `core/src/invocation.ts`, key `invocation:<id>`) **before** the command runs. A delivery that loses the claim must not execute the command; it is logged and acked. Claiming before the command is deliberate — a crash after the claim stays recoverable via the PEL.
   - The orchestrator persists its **completions cursor** in the state hash and resumes from it. A persisted non-terminal run is continued, never restarted, and an already-completed task is never re-dispatched. In-flight tasks are re-dispatched at their **persisted** attempt, so the invocation claim stays effective.
   - Never discard anything silently. A dropped task must be logged and/or routed to the DLQ; two separate bugs hid behind a silent discard.
   - Before accepting a regression test, confirm it actually **fails** against the pre-fix code.
