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

```text
           your code
              │ coord.Commit(key, value)
              ▼
        ┌─────────────┐
        │ coordinator │── WAL
        └─────────────┘
          │ gRPC    │
          ▼         ▼
     ┌────────┐ ┌────────┐
     │ cohort │ │ cohort │── WAL
     └────────┘ └────────┘
          │         │
          ▼         ▼
      Resource   Resource      your storage
```

- **Coordinator** assigns each transaction a height, collects votes from every
  cohort and decides COMMIT or ABORT. A transaction commits only if all cohorts
  vote YES.
- **Cohort** (participant) runs the protocol on one node. It does not store
  your data: it calls your `Resource` to vote on, apply or discard each transaction.
- **Resource** is your storage behind a three-method interface
  (`Prepare`/`Commit`/`Abort`), described [below](#resource).
- **WAL.** Every node logs each protocol step before acting on it. After a crash
  the coordinator finishes the interrupted transaction and redelivers undelivered
  decisions; a cohort repeats its last decision on the resource. An in-doubt
  cohort asks the coordinator for the outcome.

Nodes communicate over gRPC. The `committer` binary is the same library with a
Badger key/value store plugged in as each cohort's resource, plus a small
CLI API for its `put` and `get` commands.

## Use as a library

```bash
go get github.com/vadiminshakov/committer/v2
```

Start one coordinator and a cohort next to each piece of state that must change
atomically (packages `github.com/vadiminshakov/committer/v2/core/cohort` and
`github.com/vadiminshakov/committer/v2/core/coordinator`):

```go
participant, err := cohort.Start(ctx, cohort.Config{
    Addr:        "localhost:3001",
    Coordinator: "localhost:3000",
    DataDir:     "/var/lib/orders", // WAL; reuse it across restarts
}, ordersDB) // your cohort.Resource, see below
defer participant.Close()

coord, err := coordinator.Start(coordinator.Config{
    Addr:    "localhost:3000",
    Cohorts: []dto.Addr{"localhost:3001", "localhost:3002"},
    DataDir: "/var/lib/coordinator",
})
defer coord.Close()

height, err := coord.Commit(ctx, "order-42", payload)
switch {
case errors.Is(err, coordinator.ErrAborted):
    // a participant voted NO; nothing was applied, safe to retry
case err != nil:
    // outcome unknown for now: check coord.Decision(height) later
}
```

`Commit` returns once the COMMIT decision is durable; participants apply it in
the background and keep retrying until they succeed. Call it in the process that
runs the coordinator; the library has no client for a remote coordinator.

### Resource

A cohort runs the protocol but knows nothing about your data. At each protocol
step it calls the `cohort.Resource` you pass to `cohort.Start`: this is where
your storage decides whether it can accept a transaction and then applies or
discards it.

```go
type Resource interface {
    // Prepare votes on tx. nil is YES: Commit must then succeed eventually,
    // so reserve what it needs. An error is NO; its text reaches the caller.
    Prepare(ctx context.Context, tx dto.Tx) error
    // Commit applies tx. It must be idempotent.
    Commit(ctx context.Context, tx dto.Tx) error
    // Abort releases height. It must be idempotent and accept unknown heights.
    Abort(ctx context.Context, height uint64) error
}
```

`dto.Tx` (package `github.com/vadiminshakov/committer/v2/core/dto`) carries the
coordinator-assigned `Height` (the transaction's ID) and the `Key`/`Value`
payload passed to `coord.Commit`; their meaning is up to your resource.

**What the participant guarantees your resource:** calls arrive one at a time,
in height order. After a restart the participant repeats the last COMMIT or
ABORT, since a crash may have interrupted it. It also calls `Abort` for the next
height: the participant records its vote in the WAL only after `Prepare`
returns, so a crash in between may leave a reservation it cannot see. That vote
never reached the coordinator, so aborting is safe. A failed `Commit` or
`Abort` is retried.

[examples/transfer](examples/transfer/main.go) moves money between two banks,
each with its own resource; run it with `go run ./examples/transfer`.

## Quick start with Docker

Requires Docker with Compose:

```bash
git clone https://github.com/vadiminshakov/committer.git
cd committer
docker compose up --build --wait
docker compose exec coordinator committer put greeting hello
docker compose exec cohort committer get greeting
```

`put` writes through the coordinator; `get` reads the participant's committed
data and prints `hello`. Open [the protocol visualization](http://localhost:8080)
and press **Play**. The coordinator's CLI port is published at `localhost:4000`,
so a local `./bin/committer put greeting hello` also works; the participant is
reachable only inside the Compose network. The coordinator starts after the cohort
container. Ports 4000 and 8080 must be available.

Use `docker compose logs -f` to see node logs and `docker compose down` to stop
the nodes. Named volumes preserve their databases and WAL across restarts. To
delete the data, run `docker compose down --volumes` (this permanently removes
both nodes' data). This Compose setup is intended for local use.

## Run with the CLI

Requires **Go 1.25 or newer** and `make`. Run commands from the repository root.

```bash
make build

# Terminal 1: participant
./bin/committer cohort -nodeaddr localhost:3001 -clientaddr localhost:4001 -coordinator localhost:3000

# Terminal 2: coordinator
./bin/committer coordinator -nodeaddr localhost:3000 -clientaddr localhost:4000 -cohorts localhost:3001 -viz-port 8080

# Terminal 3: CLI
./bin/committer put --addr localhost:4000 greeting hello
./bin/committer get --addr localhost:4001 greeting
```

Nodes talk to each other on `-nodeaddr`; the CLI talks to them on `-clientaddr`, a
separate port. `put` goes to a coordinator (default `localhost:4000`) and
`get` to a cohort (default `localhost:4001`), so `--addr` can be omitted here.

`get` prints `hello`. Put CLI flags **before** key/value arguments.
Quote values containing spaces: `./bin/committer put greeting "hello world"`.
Requests have a 5-second deadline, configurable with `--timeout 10s`.

`get` reads the target cohort's committed data. If a request fails, check its error message and node logs. A timeout or lost
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
| `nodeaddr` | Node listen address for protocol traffic, `host:port` | `localhost:3050` |
| `clientaddr` | Client API listen address for `put` (coordinator) and `get` (cohort), `host:port`; empty disables it | empty |
| `coordinator` | Coordinator address; required by the `cohort` command | empty |
| `cohorts` | Comma-separated participant addresses; required by `coordinator` | empty |
| `committype` | `two-phase` or `three-phase` | `two-phase` |
| `timeout` | 3PC timeout, e.g. `1s` or `500ms`; bare numbers remain milliseconds | `1s` |
| `data-dir` | Root for the WAL and a cohort's database | `.data` |
| `viz-port` | Protocol visualization HTTP port; 0 disables it | `0` |

Timeouts must be positive whole milliseconds. CLI commands use their own
`--timeout` flag for the request deadline; it requires a duration such as `5s`.

Node startup logs show the selected role, protocol, addresses and storage paths.
Normal starts never clear data. The WAL lives beneath
`<data-dir>/wal/<role>/<address>/` and a cohort's database beneath
`<data-dir>/db/cohort/<address>/`.
Restart with the same working directory, data directory and address to reuse them.
Use an absolute `-data-dir` when launching from different working directories.

The original flag-only syntax remains supported: `-cohorts` selects the
coordinator role; otherwise the node is a cohort and needs `-coordinator`.
Prefer explicit commands for new scripts.

## Contributions

PRs and issues are welcome.

## License

[Apache License](LICENSE)
