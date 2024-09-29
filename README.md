![tests](https://github.com/vadiminshakov/committer/.github/workflows/tests.yml/badge.svg)

<p align="center">
<img src="https://github.com/vadiminshakov/committer/blob/master/committer.png">
</p>

# committer

Two-phase (2PC) and three-phase (3PC) protocols implementaion in Golang. Committer uses BadgerDB for persistence.

<br>

_protocols description:_

- http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.63.7048&rep=rep1&type=pdf (2PC)

- http://courses.cs.vt.edu/~cs5204/fall00/distributedDBMS/sreenu/3pc.html (3PC)

<br>

**Configuring**

All config parameters may be specified via command-line flags

| flag            |   description                                | default                           | example               |  
|-----------------|----------------------------------------------|-----------------------------------|-----------------------|
| role            |  role of the node (coordinator of follower)  | follower                          | -role=coordinator 
| nodeaddr        | node address                                 | localhost:3050                    | -nodeaddr=localhost:3051  
| coordinator     |  coordinator address                         |  ""                               | -coordinator=localhost:3050  
| committype      | two-phase or three-phase commit mode         | three-phase                       | -committype=two-phase  
| timeout         | timeout after which the message is considered unacknowledged (only for three-phase mode, because two-phase is blocking by design)  |  1000 |  -timeout=500
| dbpath          |  database path on filesystem                 |  ./badger                         |  -dbpath=/tmp/badger
| followers       | comma-separated list of followers' addresses | ""                                |  -followers=localhost:3052,localhost:3053,localhost:3053
| whitelist       | comma-separated list of allowed hosts        | 127.0.0.1                         |  -whitelist=192.168.0.105,192.168.0.101


<br>

example **follower**:
```
./committer -role=follower -nodeaddr=localhost:3001 -committype=three-phase -timeout=1000 -dbpath=/tmp/badger/follower
```

example **coordinator**:
```
./committer -role=coordinator -nodeaddr=localhost:3000 -followers=localhost:3001 -committype=three-phase -timeout=1000 -dbpath=/tmp/badger/coordinator
```

<br>

**Hooks**

Hooks for requests checking on Propose and Commit stage. 
It's just a function that receives *pb.ProposeRequest or *pb.CommitRequest and returns true or false.
Function body incorporate all validation logic.

Example hooks can be found at [core/cohort/commitalgo/hooks/hooks.go](https://github.com/vadiminshakov/committer/blob/master/core/cohort/commitalgo/hooks/hooks.go).
 
You can replace code in the [core/cohort/commitalgo/hooks/hooks.go](https://github.com/vadiminshakov/committer/blob/master/core/cohort/commitalgo/hooks/hooks.go) file used by committer to inject your validation logic into the handlers.

**Testing**

functional tests: `make tests`

<br>

**Testing with example client**

compile executables:
```
make prepare
```

run coordinator:
```
make run-example-coordinator
```
run follower in another shell:
```
make run-example-follower
```

run example client:
```
go run ./examples/client/client.go
```