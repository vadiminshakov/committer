![tests](https://github.com/vadiminshakov/committer/actions/workflows/tests.yml/badge.svg?branch=master)
[![Go Reference](https://pkg.go.dev/badge/github.com/vadiminshakov/committer/v2.svg)](https://pkg.go.dev/github.com/vadiminshakov/committer/v2)
[![Go Report Card](https://goreportcard.com/badge/github.com/vadiminshakov/committer)](https://goreportcard.com/report/github.com/vadiminshakov/committer)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)

<p align="center">
<img src="https://github.com/vadiminshakov/committer/blob/master/committer.png" alt="Committer Logo">
</p>

# Committer

Go implementation of **Two-Phase Commit (2PC)** and **Three-Phase Commit (3PC)** protocols for distributed systems.

Use it in two ways:

- **As a library.** Make one change commit atomically across several services:
  each service plugs its own storage into the protocol as a `Resource`.
- **As a demo cluster.** The `committer` binary runs a replicated key/value
  store with a protocol visualization, useful for learning how 2PC and 3PC behave.

## Architecture

The coordinator initiates transactions and manages the commit protocol.
Participants (called **cohorts** in the CLI) vote on each transaction and apply its outcome.
Nodes communicate over gRPC and log every protocol step to a write-ahead log (WAL),
so an interrupted transaction is finished after a crash.

## Use as a library

```bash
go get github.com/vadiminshakov/committer/v2
```

Each participant wraps the state it owns in a `Resource`:

```go
type Resource interface {
    // Prepare votes on tx. nil is YES: Commit must then succeed eventually,
    // so reserve what it needs. An error is NO; its text reaches the caller.
    Prepare(ctx context.Context, tx committer.Tx) error
    // Commit applies tx. It must be idempotent.
    Commit(ctx context.Context, tx committer.Tx) error
    // Abort releases height. It must be idempotent and accept unknown heights.
    Abort(ctx context.Context, height uint64) error
}
```

`Tx` carries the coordinator-assigned `Height` (the transaction's ID) and the
`Key`/`Value` payload passed to `Commit`; their meaning is up to your resource.

Start a participant next to each resource and one coordinator:

```go
participant, err := committer.StartParticipant(ctx, committer.ParticipantConfig{
    Addr:        "localhost:3001",
    Coordinator: "localhost:3000",
    DataDir:     "/var/lib/orders", // WAL; reuse it across restarts
}, ordersDB)
defer participant.Close()

coordinator, err := committer.StartCoordinator(committer.CoordinatorConfig{
    Addr:         "localhost:3000",
    Participants: []string{"localhost:3001", "localhost:3002"},
    DataDir:      "/var/lib/coordinator",
})
defer coordinator.Close()

height, err := coordinator.Commit(ctx, "order-42", payload)
switch {
case errors.Is(err, committer.ErrAborted):
    // a participant voted NO; nothing was applied, safe to retry
case err != nil:
    // outcome unknown for now: check coordinator.Outcome(height) later
}
```

`Commit` returns once the COMMIT decision is durable; participants apply it in
the background and keep retrying until they succeed. To send transactions to a
coordinator in another process, use `committer.Dial(addr)`.

**What the participant guarantees your resource:** calls arrive one at a time,
in height order. After a restart the participant repeats the last COMMIT or
ABORT, since a crash may have interrupted it, and calls `Abort` for a height
whose `Prepare` it had not yet logged. A failed `Commit` or `Abort` is retried.

[examples/transfer](examples/transfer/main.go) moves money between two banks,
each with its own resource; run it with `go run ./examples/transfer`.

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
To install the binary: `go install github.com/vadiminshakov/committer/v2/cmd/committer@latest`.

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

The example writes and reads five keys (`somekey0` through `somekey4`) through
`committer.Dial`, printing `got value for key 'somekey0': somevalue0`, and so on.
Customize prefixes with `-key` and `-value`. See [the example source](examples/client/client.go) for bounded
requests, error handling and closing the client connection.

## Migrating from v1

- The module path is `github.com/vadiminshakov/committer/v2`. Implementation
  packages moved to `internal/`; use the root `committer` package instead.
- The binary lives in `cmd/committer`:
  `go install github.com/vadiminshakov/committer/v2/cmd/committer@latest`.
- Hooks are gone. Validation belongs in `Resource.Prepare`, which can also
  explain a rejection; wrap a `Resource` to add metrics or auditing.
- A cohort of the binary no longer replays its whole WAL into Badger on start;
  it repeats only the last decision. Existing data directories keep working.
- A rejected `put` now returns gRPC code `Aborted`.

## Contributions

PRs and issues are welcome.

## License

[Apache License](LICENSE)
