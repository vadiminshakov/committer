![](https://github.com/vadiminshakov/committer/workflows/unit-tests/badge.svg) ![](https://github.com/vadiminshakov/committer/workflows/functional-tests/badge.svg)
# committer

Two-phase (2PC) and three-phase (3PC) protocols implementaion in Golang. Committer uses BadgerDB for persistence.

<br>

_protocols description:_

- http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.63.7048&rep=rep1&type=pdf (2PC)

- http://courses.cs.vt.edu/~cs5204/fall00/distributedDBMS/sreenu/3pc.html (3PC)

<br>

**TODO**

Pluggable hooks for requests checking

<br>

**Configuring using config file**

Use `./config/config.yaml` as configuration with command:
 ```
 ./committer -config=config
```
 
_(the committer looks for configuration files in the ./config directory)_

<br>

**Configuring using command-line flags**

All config parameters may be specified via command-line flags

| flag  |   description| example value  |  
|---|---|---|
| config  |  path to config |  config (means config.yaml in the ./config/ dir) |
| role  |  role of the node (coordinator of follower) | 'follower' or 'coordinator'  | 
| nodeaddr  | node address | localhost:3051 |   
| coordinator  |  coordinator address |  localhost:3050 |   
| committype  | two-phase or three-phase commit mode | 'two-phase' or 'three-phase' |  
| timeout  | timeout after which the message is considered unacknowledged (only for three-phase mode, because two-phase is blocking by design)  |  1000 |  
| dbpath  |  database path on filesystem |  /tmp/badger |  
|hooks| path to shared object file with hooks | hooks/src/hooks.so |

<br>

example **follower**:
```
./committer -role=follower -nodeaddr=localhost:3001 -committype=three-phase -timeout=1000 -dbpath=/tmp/badger/follower
```

example **coordinator**:
```
./committer -role=coordinator -nodeaddr=localhost:3000 -follower=localhost:3001 -committype=three-phase -timeout=1000 -dbpath=/tmp/badger/coordinator
```

<br>

**Testing**

functional tests: `make functional-tests`

unit-tests: `make unit-tests`

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