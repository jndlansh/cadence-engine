# Cadence Engine — Architecture, Engineering Spec & Agent Guidelines

## 1. Project Mission & System Overview
Cadence is an asynchronous, distributed, fault-tolerant DAG (Directed Acyclic Graph) workflow orchestration engine built end-to-end in TypeScript. It coordinates arbitrary dependency-driven shell and container jobs across decoupled worker processes with rigorous execution guarantees:
- **Strict Topological Invariant:** No child task can execute before all parent dependencies emit clean success exit codes (`0`).
- **Deterministic Cycle Rejection:** Invalid cyclic pipelines are detected and rejected at submission time using Kahn’s Algorithm ($O(V + E)$).
- **At-Least-Once Delivery & Failure Recovery:** Tasks lost to sudden worker crashes (`kill -9`, hardware loss, network partition) are identified via missing heartbeats and claimed from the Redis Pending Entries List (PEL) without dropping jobs.
- **Idempotency Safeguards:** Every task attempt receives a deterministic invocation identifier (`run_<workflow_id>_<task_id>_<attempt>`) to prevent duplicate executions from producing side-effects.

---

## 2. Monorepo Repository Structure
Managed via `pnpm` workspaces:
```text
cadence/
├── ARCHITECTURE.md                  # System design contract, phases & agent rules
├── docker-compose.yml              # Local infrastructure container (Redis 7)
├── package.json                    # Workspace root scripts & tooling
├── pnpm-workspace.yaml             # Workspace definitions
├── tsconfig.json                   # Shared TypeScript compiler configuration
├── workflows/                       # Example workflow YAML manifests
│   └── data-pipeline.yaml          # Sample multi-stage DAG workflow
└── packages/
    ├── core/                        # Pure domain engine, schemas & process execution
    │   ├── src/types.ts             # Zod contracts & shared interfaces
    │   ├── src/dag.ts               # Kahn's algorithm & dependency traversal
    │   ├── src/dag.test.ts          # Vitest suite for graph invariants
    │   ├── src/parser.ts            # YAML deserialization & runtime validation
    │   ├── src/parser.test.ts       # Vitest suite for YAML loading & edge cases
    │   ├── src/runner.ts            # Subprocess wrapper, stdout/stderr & timeouts
    │   ├── src/runner.test.ts       # Vitest suite for task execution & failure handling
    │   └── src/index.ts             # Core package barrel export
    ├── orchestrator/                # Coordination layer & scheduler daemon
    │   ├── src/scheduler.ts         # Redis Streams task dispatcher (XADD)
    │   ├── src/state.ts             # Global workflow run state manager
    │   ├── src/reclaimer.ts         # Stale task cleaner (XCLAIM & worker health)
    │   └── src/index.ts             # Orchestrator entry point
    └── worker/                      # Independent task runner node
        ├── src/consumer.ts          # Redis Streams consumer loop (XREADGROUP)
        ├── src/heartbeat.ts         # Periodic liveness beacon (Redis TTL keys)
        └── src/index.ts             # Worker CLI entry point

## 3. Core Architectural Phases & Roadmap

### Phase 1: Core Domain Engine & Graph Logic `[STATUS: COMPLETED]`

- [x] Defined Zod validation schemas (`TaskSchema`, `WorkflowSchema`) ensuring type-safe boundaries at runtime.
- [x] Implemented Kahn's Algorithm in `DAGResolver` to achieve $O(V + E)$ cycle detection and topological sorting.
- [x] Referential integrity enforcement: workflows fail fast if a task depends on an undefined node ID.
- [x] Dynamic child task evaluation (`getNextExecutableTasks`) based on completed task sets.
- [x] Implemented YAML parsing and validation bridge (`parseWorkflowYAML`).
- [x] Full unit test coverage passing in Vitest.

### Phase 2: Process Execution Layer (`TaskRunner`) `[STATUS: IN PROGRESS]`

- [ ] Child process execution engine using Node.js `child_process.spawn`.
- [ ] Cross-platform shell wrapping (`cmd.exe /c` on Windows, `sh -c` on POSIX).
- [ ] Process timeout guard: forcefully terminates hung processes (`SIGKILL`) after `timeout_ms`.
- [ ] Buffered stdout and stderr capture with normalized exit codes.
- [ ] Execution unit tests validating exit codes, log streaming, and timeout interruptions.

### Phase 3: Distributed State & Queue Infrastructure (Redis Streams)

- [ ] Redis client initialization and connection management using `ioredis`.
- [ ] Task queuing primitive: publish unlocked tasks to Redis Stream `cadence:tasks` using `XADD`.
- [ ] Consumer Group configuration (`XGROUP CREATE`) enabling parallel, load-balanced worker consumption.
- [ ] Explicit delivery acknowledgment (`XACK`) once a task exits cleanly.
- [ ] Dead Letter Queue (DLQ) routing for tasks exceeding maximum configured retries.

### Phase 4: Heartbeats, Task Reclaiming & Worker Fault Tolerance

- [ ] Worker Liveness Beacon: workers maintain an ephemeral key with TTL in Redis (`SET worker:<id>:heartbeat "alive" EX 10`) renewed every 3 seconds.
- [ ] Orchestrator Reclaimer Loop: background sweeper inspecting the Pending Entries List (PEL) via `XPENDING`.
- [ ] Automatic Failover (`XCLAIM`): if a task is un-acked and its assigned worker's heartbeat key expires, reassign the task to an active worker.
- [ ] Exponential backoff scheduler for failed tasks using Redis Sorted Sets (`ZSET`).

### Phase 5: Workflow State Machine & Lifecycle Persistence

- [ ] Workflow lifecycle state management: `PENDING` $\to$ `RUNNING` $\to$ `COMPLETED` | `FAILED`.
- [ ] Tracking task execution attempts, execution latency, and error states.
- [ ] Structured file/console event logger.

### Phase 6: Orchestrator CLI & Workflow Submission

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

4. **Mandatory Test-Driven Quality**
   - Every algorithmic change, edge-case fix, or utility function must include a unit test in a corresponding `*.test.ts` file.
   - All changes must pass `pnpm test` with zero failures before being considered complete.