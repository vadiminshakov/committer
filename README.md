![tests](https://github.com/vadiminshakov/committer/actions/workflows/tests.yml/badge.svg?branch=master)
[![Go Reference](https://pkg.go.dev/badge/github.com/vadiminshakov/committer.svg)](https://pkg.go.dev/github.com/vadiminshakov/committer)
[![Go Report Card](https://goreportcard.com/badge/github.com/vadiminshakov/committer)](https://goreportcard.com/report/github.com/vadiminshakov/committer)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)

<p align="center">
<img src="https://github.com/vadiminshakov/committer/blob/master/committer.png" alt="Committer Logo">
</p>

# Committer

Go implementation of **Two-Phase Commit (2PC)** and **Three-Phase Commit (3PC)** protocols for distributed systems.

## Architecture

The coordinator initiates transactions and manages the commit protocol.
Participants (called **cohorts** in the code and CLI) vote on each transaction and apply its outcome. Nodes communicate over gRPC and persist state using a database and write-ahead log (WAL).

## Quick start with Docker

Requires Docker with Compose:

```bash
git clone https://github.com/vadiminshakov/committer.git
cd committer
docker compose up --build --wait
docker compose exec coordinator committer put --addr 127.0.0.1:3000 greeting hello
docker compose exec coordinator committer get --addr 127.0.0.1:3000 greeting
```

The last command prints `hello`. Open [the protocol visualization](http://localhost:8080)
and press **Play**. The coordinator's gRPC port is available at `localhost:3000`;
the participant is reachable only inside the Compose network. The coordinator starts
after the cohort container. The sample write checks the full transaction path.
Ports 3000 and 8080 must be available.

Use `docker compose logs -f` to see node logs and `docker compose down` to stop
the nodes. Named volumes preserve their databases and WAL across restarts. To
delete the data, run `docker compose down --volumes` (this permanently removes
both nodes' data). This Compose setup is intended for local use.

## Quick start with Go

Requires **Go 1.25 or newer** and `make`. Run commands from the repository root.

```bash
git clone https://github.com/vadiminshakov/committer.git
cd committer
make demo
```

This builds `bin/committer`, starts a coordinator and one participant (cohort)
using 2PC, writes `greeting=hello`, and reads it back:

```text
Committed transaction 0
hello
```

The transaction number and height increase on subsequent runs. Open
[the protocol visualization](http://localhost:8080) and press **Play** to see the
message exchange. Press **Ctrl+C** in the terminal to stop both demo nodes.
Ports 3000, 3001 and 8080 must be available. Logs are in `.data/demo/logs/`.

Demo data survives restarts in `.data/demo/`. To start from scratch, stop all
demo nodes, then run `make demo-reset`. This deletes **only demo data**.

### Run nodes yourself

```bash
make build

# Terminal 1: participant
./bin/committer cohort -nodeaddr localhost:3001 -coordinator localhost:3000

# Terminal 2: coordinator
./bin/committer coordinator -nodeaddr localhost:3000 -cohorts localhost:3001 -viz-port 8080

# Terminal 3: client
./bin/committer put --addr localhost:3000 greeting hello
./bin/committer get --addr localhost:3000 greeting
```

`get` prints `hello`. Put client flags **before** key/value arguments.
Quote values containing spaces: `./bin/committer put greeting "hello world"`.
Requests have a 5-second deadline, configurable with `--timeout 10s`.

`put` requires a coordinator. `get` reads the target node's local committed data.
If a request fails, check its error message and node logs. A timeout or lost
connection during `put` does not by itself establish whether the transaction committed.

For 3PC, pass `-committype three-phase -timeout 1s` to **both** nodes.
Run `./bin/committer --help` or `./bin/committer coordinator -h` for help.

## Protocol visualization

The optional web UI animates protocol messages and shows an event log, transaction
height, key and participants. Use Play/Pause, speed and replay controls to inspect
the exchange. It is a protocol demonstration, not a metrics or health dashboard.

Enable it on a node with `-viz-port 8080`, then open `http://localhost:8080`.
Use a different port for each node's visualization. The HTTP server listens on
all interfaces; the displayed localhost URL is for local access.

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

Node commands accept these flags:

| Flag | Description | Default |
|------|-------------|---------|
| `nodeaddr` | Node listen address, `host:port` | `localhost:3050` |
| `coordinator` | Coordinator address; required by the `cohort` command | empty |
| `cohorts` | Comma-separated participant addresses; required by `coordinator` | empty |
| `committype` | `two-phase` or `three-phase` | `two-phase` |
| `timeout` | 3PC timeout, e.g. `1s` or `500ms`; bare numbers remain milliseconds | `1s` |
| `data-dir` | Root for persistent databases and WAL | `.data` |
| `viz-port` | Protocol visualization HTTP port; 0 disables it | `0` |

Timeouts must be positive whole milliseconds. Client commands use their own
`--timeout` flag for the request deadline; it requires a duration such as `5s`.

Node startup logs show the selected role, protocol, addresses and storage paths.
Normal starts never clear data. Databases and WAL live beneath
`<data-dir>/db/<role>/<address>/` and `<data-dir>/wal/<role>/<address>/`.
Restart with the same working directory, data directory and address to reuse them.
Use an absolute `-data-dir` when launching from different working directories.

The original flag-only syntax remains supported: `-cohorts` selects the
coordinator role; otherwise the node is a cohort. Prefer explicit commands for
new scripts because their required and incompatible flags are validated.

## Go client example

With both nodes running:

```bash
go run ./examples/client -addr localhost:3000 -timeout 5s
```

The example writes and reads five keys (`somekey0` through `somekey4`), printing
`got value for key 'somekey0': somevalue0`, and so on. Customize prefixes with
`-key` and `-value`. See [the example source](examples/client/client.go) for bounded
requests, error handling and closing the client connection.

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

## Contributions

PRs and issues are welcome.

## License

[Apache License](LICENSE)
