![tests](https://github.com/vadiminshakov/committer/actions/workflows/tests.yml/badge.svg?branch=master)
[![Go Reference](https://pkg.go.dev/badge/github.com/vadiminshakov/committer.svg)](https://pkg.go.dev/github.com/vadiminshakov/committer)
[![Go Report Card](https://goreportcard.com/badge/github.com/vadiminshakov/committer)](https://goreportcard.com/report/github.com/vadiminshakov/committer)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)

<p align="center">
<img src="https://github.com/vadiminshakov/committer/blob/master/committer.png" alt="Committer Logo">
</p>

# Committer

Go implementation of **Two-Phase Commit (2PC)** and **Three-Phase Commit (3PC)** protocols for distributed systems.

## **Architecture**

The system consists of two types of nodes: **Coordinator** and **Cohorts**.
The **Coordinator** is responsible for initiating and managing the commit protocols (2PC or 3PC), while the **Cohorts** participate in the protocol by responding to the coordinator's requests.
The communication between nodes is handled using gRPC, and the state of each node is managed using a state machine.

## Monitoring & Visualization

Pass `-viz-port=<port>` to enable the web dashboard with live charts for throughput, commit/abort rates, latency, and node health:

```bash
./committer -nodeaddr=localhost:3000 -cohorts=localhost:3001 -viz-port=8080
```

Then open `http://localhost:8080` in a browser.

<p align="center">
<img src="https://github.com/vadiminshakov/committer/blob/master/dashboard.png" alt="Committer Dashboard">
</p>

## **Atomic Commit Protocols**

### **Two-Phase Commit (2PC)**

The Two-Phase Commit protocol ensures atomicity in distributed transactions through two distinct phases:

#### **Phase 1: Voting Phase (Propose)**
1. **Coordinator** sends a `PROPOSE` request to all cohorts with transaction data
2. Each **Cohort** validates the transaction locally and responds:
   - `ACK` (Yes) - if ready to commit
   - `NACK` (No) - if unable to commit
3. **Coordinator** waits for all responses

#### **Phase 2: Commit Phase**
1. If **all cohorts** voted `ACK`:
   - **Coordinator** sends `COMMIT` to all cohorts
   - Each **Cohort** commits the transaction and responds with `ACK`
2. If **any cohort** voted `NACK`:
   - **Coordinator** sends `ABORT` to all cohorts
   - Each **Cohort** aborts the transaction

### **Three-Phase Commit (3PC)**

The Three-Phase Commit protocol extends 2PC with an additional phase to reduce blocking scenarios:

#### **Phase 1: Voting Phase (Propose)**
1. **Coordinator** sends `PROPOSE` request to all cohorts
2. **Cohorts** respond with `ACK`/`NACK` (same as 2PC)

#### **Phase 2: Preparation Phase (Precommit)**
1. If all cohorts voted `ACK`:
   - **Coordinator** sends `PRECOMMIT` to all cohorts
   - **Cohorts** acknowledge they're prepared to commit
   - **Timeout mechanism**: If cohort doesn't receive `COMMIT` within timeout, it auto-commits
2. If any cohort voted `NACK`:
   - **Coordinator** sends `ABORT` to all cohorts

#### **Phase 3: Commit Phase**
1. **Coordinator** sends `COMMIT` to all cohorts
2. **Cohorts** perform the actual commit operation

## Configuration

| Flag          | Description                                                  | Default        |
|---------------|--------------------------------------------------------------|----------------|
| `nodeaddr`    | Address of the current node                                  | `localhost:3050` |
| `coordinator` | Coordinator address (cohorts only)                           | `""`           |
| `committype`  | `two-phase` or `three-phase`                                 | `three-phase`  |
| `timeout`     | Timeout (ms) for unacknowledged messages (3PC only)          | `1000`         |
| `cohorts`     | Comma-separated cohort addresses (presence implies coordinator role) | `""` |
| `viz-port`    | Port for web dashboard (0 = disabled)                        | `0`            |

## Usage

```bash
# Coordinator
./committer -nodeaddr=localhost:3000 -cohorts=localhost:3001 -committype=three-phase

# Cohort
./committer -nodeaddr=localhost:3001 -coordinator=localhost:3000 -committype=three-phase
```

## Hooks

Custom validation and business logic for the **Propose** and **Commit** stages. Hooks run in registration order; returning `false` rejects the operation.

```go
committer := commitalgo.NewCommitter(database, "three-phase", wal, timeout,
    hooks.NewMetricsHook(),
    hooks.NewValidationHook(100, 1024),
    hooks.NewAuditHook("audit.log"),
)

// or register later
committer.RegisterHook(myCustomHook)
```

## Testing

```bash
make tests
```

To test with the example client:

```bash
make prepare
make run-example-coordinator   # terminal 1
make run-example-cohort        # terminal 2
make run-example-client        # terminal 3
```

## Contributions

PRs and issues are welcome.

## License

[Apache License](LICENSE)
