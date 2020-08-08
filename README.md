![](https://github.com/vadiminshakov/committer/workflows/unit-tests/badge.svg) ![](https://github.com/vadiminshakov/committer/workflows/functional-tests/badge.svg)
# committer

Two-phase (2PC) and three-phase (3PC) protocols implementaion in Golang.

_instructions will be provided soon_

**TODO**

In three-phase mode followers should commit anyway after precommit phase, even if they didn't receive commit msg from coordinator (non-blocking protocol).