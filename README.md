![tests](https://github.com/vadiminshakov/committer/actions/workflows/tests.yml/badge.svg?branch=master)
[![Go Reference](https://pkg.go.dev/badge/github.com/vadiminshakov/committer.svg)](https://pkg.go.dev/github.com/vadiminshakov/committer)
[![Go Report Card](https://goreportcard.com/badge/github.com/vadiminshakov/committer)](https://goreportcard.com/report/github.com/vadiminshakov/committer)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)

<p align="center">
<img src="https://github.com/vadiminshakov/committer/blob/master/committer.png" alt="Committer Logo">
</p>

# **Committer**

**Committer** is a Go implementation of the **Two-Phase Commit (2PC)** and **Three-Phase Commit (3PC)** protocols for distributed systems.

## **Architecture**

The system consists of two types of nodes: **Coordinator** and **Cohorts**.
The **Coordinator** is responsible for initiating and managing the commit protocols (2PC or 3PC), while the **Cohorts** participate in the protocol by responding to the coordinator's requests.
The communication between nodes is handled using gRPC, and the state of each node is managed using a state machine.

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

### **Protocol Comparison**

| Aspect | 2PC | 3PC |
|--------|-----|-----|
| **Phases** | 2 (Propose → Commit) | 3 (Propose → Precommit → Commit) |
| **Blocking** | Yes (during phase 2) | Reduced (timeout auto-commit) |
| **Fault Tolerance** | Coordinator failure blocks | Better resilience to failures |

## **Key Features**

- **2PC and 3PC support**: Implements two widely used atomic commit protocols for distributed transactions.
- **Persistence**: Uses **BadgerDB** and WAL for reliable data storage and transaction logs.
- **Configurable**: All options can be specified using command-line flags.
- **Flexible Hook System**: Extensible hook system for custom validation, metrics, auditing, and business logic without code changes.
- **Built-in Hooks**: Ready-to-use hooks for metrics collection, validation, and audit logging.
- **Timeout Handling**: Automatic timeout management for 3PC with proper skip record handling in WAL.
- **gRPC-based communication**: Efficient inter-node communication using gRPC.

## **Configuration**

All configuration parameters can be set using command-line flags:

| **Flag**       | **Description**                                          | **Default**         | **Example**                          |
|-----------------|---------------------------------------------------------|---------------------|-------------------------------------|
| `role`         | Node role: `coordinator` or `cohort`                    | `cohort`            | `-role=coordinator`                 |
| `nodeaddr`     | Address of the current node                             | `localhost:3050`    | `-nodeaddr=localhost:3051`          |
| `coordinator`  | Coordinator address (required for cohorts)              | `""`                | `-coordinator=localhost:3050`       |
| `committype`   | Commit protocol: `two-phase` or `three-phase`           | `three-phase`       | `-committype=two-phase`             |
| `timeout`      | Timeout (ms) for unacknowledged messages (3PC only)     | `1000`              | `-timeout=500`                      |
| `dbpath`       | Path to the BadgerDB database on the filesystem         | `./badger`          | `-dbpath=/tmp/badger`               |
| `cohorts`      | Comma-separated list of cohort addresses                | `""`                | `-cohorts=localhost:3052,3053`      |
| `whitelist`    | Comma-separated list of allowed hosts                   | `127.0.0.1`         | `-whitelist=192.168.0.1,192.168.0.2`|


## **Usage**

### **Running as a Cohort**
```bash
./committer -role=cohort -nodeaddr=localhost:3001 -coordinator=localhost:3000 -committype=three-phase -timeout=1000 -dbpath=/tmp/badger/cohort
```

### **Running as a Coordinator**
```bash
./committer -role=coordinator -nodeaddr=localhost:3000 -cohorts=localhost:3001 -committype=three-phase -timeout=1000 -dbpath=/tmp/badger/coordinator
```

## **Hooks System**

The hooks system allows you to add custom validation and business logic during the **Propose** and **Commit** stages without modifying the core code. Hooks are executed in the order they were registered, and if any hook returns `false`, the operation is rejected.

```go
// Default usage (with built-in default hook)
committer := commitalgo.NewCommitter(database, "three-phase", wal, timeout)

// With custom hooks
metricsHook := hooks.NewMetricsHook()
validationHook := hooks.NewValidationHook(100, 1024)
auditHook := hooks.NewAuditHook("audit.log")

committer := commitalgo.NewCommitter(database, "three-phase", wal, timeout,
    metricsHook,
    validationHook,
    auditHook,
)

// Dynamic registration
committer.RegisterHook(myCustomHook)
```

## **Testing**

### **Run Functional Tests**
```bash
make tests
```

### **Testing with Example Client**
1. Compile executables:

```bash
make prepare
```

2. Run the coordinator:

```bash
make run-example-coordinator
```

3. Run a cohort in another terminal:

```bash
make run-example-cohort
```

4. Start the example client:

```bash
make run-example-client
```

Or directly:

```bash
go run ./examples/client/client.go
```

## **Contributions**

Contributions are welcome! Feel free to submit a PR or open an issue if you find bugs or have suggestions for improvement.

## **License**

This project is licensed under the [Apache License](LICENSE).