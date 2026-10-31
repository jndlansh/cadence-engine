<div align="center">

# ⚡ CADENCE

### Distributed, Fault-Tolerant DAG Workflow Orchestration Engine

[![TypeScript](https://img.shields.io/badge/TypeScript-5.x-3178C6?style=for-the-badge&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Redis](https://img.shields.io/badge/Redis-7.x-DC382D?style=for-the-badge&logo=redis&logoColor=white)](https://redis.io/)
[![Vitest](https://img.shields.io/badge/Vitest-Passing-6E9F18?style=for-the-badge&logo=vitest&logoColor=white)](https://vitest.dev/)
[![pnpm](https://img.shields.io/badge/pnpm-Workspace-F69220?style=for-the-badge&logo=pnpm&logoColor=white)](https://pnpm.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=for-the-badge)](LICENSE)

<p align="center">
  <b>Cadence</b> is an asynchronous, event-driven orchestration engine designed to execute complex, dependency-bound task graphs across decoupled worker clusters with mathematical correctness and zero dropped jobs.
</p>

[Architecture](#-architectural-overview) •
[Core Guarantees](#-core-guarantees) •
[Workspace Topology](#-workspace-topology) •
[Quickstart](#-quickstart) •
[Engine Lifecycle](#-engine-lifecycle)

---

</div>

## 🌐 Overview

In high-throughput distributed systems, orchestrating multi-stage workloads requires balancing strict topological dependencies against unpredictable machine failures. Traditional queue orchestrators either introduce heavy database overhead or lack resilience against sudden worker crashes.

**Cadence** resolves this by decoupling workflow declaration from physical compute:
- **Declarative DAGs:** Pipelines are defined via YAML specifications, type-enforced at runtime, and statically checked for cycles before scheduling.
- **Master-Worker Topology:** An autonomous orchestrator traverses graph dependencies and dispatches ready tasks onto **Redis Streams**.
- **Self-Healing Compute:** Independent workers pull workloads through distributed consumer groups. If a node fails mid-execution, orphaned jobs are dynamically reclaimed and reassigned with zero human intervention.

---

## 🛡️ Core Guarantees

```text
┌────────────────────────────────────────────────────────────────────────┐
│                          DESIGN INVARIANTS                             │
├────────────────────────────┬───────────────────────────────────────────┤
│ 📐 Topological Strictness   │ Zero child tasks execute until all parent │
│                            │ dependencies emit exit code 0.            │
├────────────────────────────┼───────────────────────────────────────────┤
│ 🔁 Deterministic Cycle     │ Cyclic graphs are rejected at the parsing │
│    Rejection               │ boundary via Kahn's Algorithm: O(V + E).  │
├────────────────────────────┼───────────────────────────────────────────┤
│ 🔄 At-Least-Once Delivery   │ Work is acknowledged only post-completion │
│                            │ via explicit atomic XACK semantics.       │
├────────────────────────────┼───────────────────────────────────────────┤
│ 🩺 Autonomous Failover     │ Dead workers are identified via TTL keys; │
│                            │ tasks are recovered using Redis XCLAIM.   │
├────────────────────────────┼───────────────────────────────────────────┤
│ 🛡️ Subprocess Isolation    │ Tasks run inside isolated shells with hard│
│                            │ process kill guards on timeout bounds.    │
└────────────────────────────┴───────────────────────────────────────────┘
```

---

## 🏗️ Architectural Overview

Cadence separates coordination state from execution runtimes through three clean abstractions:

```
                        ┌────────────────────────┐
                        │   YAML Pipeline Spec   │
                        └───────────┬────────────┘
                                    │
                                    ▼
                   ┌──────────────────────────────────┐
                   │        @cadence/core             │
                   │  - Zod Runtime Validation        │
                   │  - Kahn's Algorithm (O(V+E))     │
                   │  - Dynamic In-Degree Traversal   │
                   └────────────────┬─────────────────┘
                                    │
                                    ▼
                   ┌──────────────────────────────────┐
                   │     @cadence/orchestrator        │
                   │  - State Machine Transition      │
                   │  - Pending Entries Reclaimer     │
                   │  - Dispatcher (XADD)             │
                   └──────────┬──────────────▲────────┘
                              │              │
                   cadence:tasks (XADD)      cadence:heartbeats / ACK
                              │              │
              ┌───────────────┴──────────────┴───────────────┐
              │                                              │
              ▼                                              ▼
    ┌────────────────────┐                         ┌────────────────────┐
    │  @cadence/worker 1 │                         │  @cadence/worker 2 │
    │  - Consumer Group  │                         │  - Consumer Group  │
    │  - TaskRunner      │                         │  - TaskRunner      │
    │  - Heartbeat Loop  │                         │  - Heartbeat Loop  │
    └────────────────────┘                         └────────────────────┘
```

### Key Subsystems

1. **Topological Engine (`@cadence/core`):** Pure, deterministic domain logic. Manages graph representations, evaluates child unblocking dynamically, and wraps shell executions with timeout safeguards.
2. **Orchestrator Daemon (`@cadence/orchestrator`):** The brain of the cluster. Ingests workflows, manages active pipeline lifecycles, and maintains cluster resilience via periodic Pending Entries List (PEL) reclamation sweeps.
3. **Execution Workers (`@cadence/worker`):** Stateless worker nodes joining the `cadence:workers` Redis consumer group. Executes commands in cross-platform shells (`cmd.exe` / `sh`), streams execution logs, and emits liveness heartbeats.

---

## 📂 Workspace Topology

Cadence is structured as a lightweight, zero-bloat monorepo powered by `pnpm` workspaces:

```text
cadence/
├── ARCHITECTURE.md           # Formal technical specification & AI guardrails
├── workflows/                # Sample declarative workflow manifests
│   └── data-pipeline.yaml    # Reference multi-stage DAG
└── packages/
    ├── core/                 # Pure domain: Schemas, Kahn's DAG, TaskRunner
    │   ├── src/types.ts      # Shared Zod contracts & domain interfaces
    │   ├── src/dag.ts        # Graph validation & topological sorting
    │   ├── src/parser.ts     # YAML parser & schema enforcement
    │   └── src/runner.ts     # Subprocess wrapper, timeouts & log streams
    ├── orchestrator/         # Scheduler, state machine, & failure reclaimer
    │   ├── src/scheduler.ts  # Stream producer & task dispatch engine
    │   └── src/reclaimer.ts  # Worker health monitor & XCLAIM engine
    └── worker/               # Autonomous execution consumer
        ├── src/consumer.ts   # Consumer group loop (XREADGROUP & XACK)
        └── src/heartbeat.ts  # Ephemeral liveness beacon (TTL keys)
```

---

## 🚀 Quickstart

### Prerequisites

- **Node.js**: `v20.x` or higher
- **Package Manager**: `pnpm` (`npm install -g pnpm`)
- **Redis Server**: Redis `v7.x+` (running locally via WSL 2, Docker, or native daemon)

### 1. Installation

Clone the repository and install all workspace dependencies:

```bash
git clone https://github.com/jndlansh/cadence.git
cd cadence
pnpm install
```

### 2. Verify Graph Engine & Tests

Run the full cross-platform test suite (covering graph algorithms, cycle detection, parser validation, and subprocess execution):

```bash
pnpm test
```

### 3. Start Redis

Ensure your Redis instance is listening on `127.0.0.1:6379`:

```bash
# Using WSL 2 / Linux service
sudo service redis-server start

# Verify connection
redis-cli ping
# Output: PONG
```

---

## 📋 Declarative Pipeline Example

Pipelines are declared in human-readable YAML with explicit dependency relationships and execution controls:

```yaml
version: "1.0"
workflow: telemetry_etl_pipeline

tasks:
  - id: extract_source
    command: 'node -e "console.log(\"Extracting raw events...\")"'
    timeout_ms: 10000
    retries: 2

  - id: transform_metrics
    command: 'node -e "console.log(\"Aggregating metrics...\")"'
    depends_on:
      - extract_source
    timeout_ms: 15000

  - id: generate_report
    command: 'node -e "console.log(\"Publishing final report...\")"'
    depends_on:
      - transform_metrics
    timeout_ms: 5000
```

---

## 🔄 Engine Lifecycle

```text
[ Submit YAML ]
       │
       ▼
[ Parse & Validate ] ──(Invalid / Cycle)──► [ Reject Pipeline Immediately ]
       │
       ▼ (Valid DAG)
[ Extract In-Degree 0 Tasks ]
       │
       ▼
[ Dispatch to Stream (XADD) ] ◄──────────────────────────────┐
       │                                                     │
       ▼                                                     │
[ Worker Claims Task (XREADGROUP) ]                          │
       │                                                     │
       ├─► [ Emitting Heartbeats (TTL Keys) ]                 │
       │                                                     │
       ▼                                                     │
[ Execute via Subprocess Runner ]                            │
       │                                                     │
       ├──(Timeout Exceeded)──► [ Force SIGKILL (Exit 124) ] │
       │                                                     │
       ├──(Success: Exit 0)───► [ XACK ] ──► [ Unblock Children ]
       │
       └──(Failure: Non-Zero)
              │
              ├──(Attempt < Retries)──► [ Exponential Backoff Delay ]
              │
              └──(Retries Exhausted)──► [ Move to Dead Letter Stream (DLQ) ]
```

---

## 🛣️ Engineering Roadmap

- [x] **Phase 1: Core Domain Engine** — Zod schema boundaries, Kahn's algorithm cycle detection, dynamic unblocking, and YAML deserialization.
- [x] **Phase 2: Execution Subsystem** — Subprocess runner, cross-platform shell standardizations, hard timeout guards, and stdio buffering.
- [x] **Phase 3: Redis Distributed Queue** — Redis Streams dispatcher, dynamic worker consumer groups, and atomic acknowledgment contracts.
- [x] **Phase 4: Fault Recovery & Reclaim Loops** — Pending Entries List (PEL) monitoring, worker TTL heartbeats, autonomous `XCLAIM` failovers, and exponential-backoff retry scheduling via Redis sorted sets.
- [ ] **Phase 5: Workflow State & Event Telemetry** — Pipeline state transitions, execution history persistence, and structured run telemetry.
- [ ] **Phase 6: Orchestrator CLI** — Unified terminal interface to trigger workflows, inspect queues, and observe live cluster progress.

---

## 📜 License

Distributed under the **MIT License**. See [LICENSE](LICENSE) for details.
