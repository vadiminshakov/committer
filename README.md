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

The system consists of two types of nodes: **Coordinator** and **Followers**.
The **Coordinator** is responsible for initiating and managing the commit protocols (2PC or 3PC), while the **Followers** participate in the protocol by responding to the coordinator's requests.
The communication between nodes is handled using gRPC, and the state of each node is managed using a state machine.

## **Key Features**

- **2PC and 3PC support**: Implements two widely used consensus protocols for distributed transactions.
- **Persistence**: Uses **BadgerDB** and WAL for reliable data storage and transaction logs.
- **Configurable**: All options can be specified using command-line flags.
- **Hooks for validation**: Custom logic can be injected during **Propose** and **Commit** stages.
- **gRPC-based communication**: Efficient inter-node communication using gRPC.

## **Configuration**

All configuration parameters can be set using command-line flags:

| **Flag**       | **Description**                                          | **Default**         | **Example**                          |
|-----------------|---------------------------------------------------------|---------------------|-------------------------------------|
| `role`         | Node role: `coordinator` or `follower`                  | `follower`          | `-role=coordinator`                 |
| `nodeaddr`     | Address of the current node                             | `localhost:3050`    | `-nodeaddr=localhost:3051`          |
| `coordinator`  | Coordinator address (required for followers)            | `""`                | `-coordinator=localhost:3050`       |
| `committype`   | Commit protocol: `two-phase` or `three-phase`           | `three-phase`       | `-committype=two-phase`             |
| `timeout`      | Timeout (ms) for unacknowledged messages (3PC only)     | `1000`              | `-timeout=500`                      |
| `dbpath`       | Path to the BadgerDB database on the filesystem         | `./badger`          | `-dbpath=/tmp/badger`               |
| `followers`    | Comma-separated list of follower addresses              | `""`                | `-followers=localhost:3052,3053`    |
| `whitelist`    | Comma-separated list of allowed hosts                   | `127.0.0.1`         | `-whitelist=192.168.0.1,192.168.0.2`|


## **Usage**

### **Running as a Follower**
```bash
./committer -role=follower -nodeaddr=localhost:3001 -committype=three-phase -timeout=1000 -dbpath=/tmp/badger/follower
```

### **Running as a Coordinator**
```bash
./committer -role=coordinator -nodeaddr=localhost:3000 -followers=localhost:3001 -committype=three-phase -timeout=1000 -dbpath=/tmp/badger/coordinator
```

## **Hooks**

Hooks allow you to add custom validation logic during the **Propose** and **Commit** stages.  
A hook is a function that accepts `*pb.ProposeRequest` or `*pb.CommitRequest` and returns a boolean.

Example hook implementation can be found [here](https://github.com/vadiminshakov/committer/blob/master/core/cohort/commitalgo/hooks/hooks.go).

To inject your own logic, replace the code in the hook file with your custom validation.
markdown
Copy code

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

3. Run a follower in another terminal:

```bash
make run-example-follower
```

4. Start the example client:

```bash
go run ./examples/client/client.go
```

## **Contributions**

Contributions are welcome! Feel free to submit a PR or open an issue if you find bugs or have suggestions for improvement.

## **License**

This project is licensed under the [Apache License](LICENSE).