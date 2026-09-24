#!/bin/sh
# Run from the repository root via make demo. Data survives restarts.
set -eu
bin=./bin/committer
mkdir -p .data/demo/logs
cohort_pid=
coordinator_pid=
cleanup() {
    trap - EXIT INT TERM
    for pid in "$coordinator_pid" "$cohort_pid"; do
        if [ -n "$pid" ]; then kill -TERM "$pid" 2>/dev/null || true; fi
    done
    for pid in "$coordinator_pid" "$cohort_pid"; do
        if [ -n "$pid" ]; then wait "$pid" 2>/dev/null || true; fi
    done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

wait_node() {
    addr=$1
    pid=$2
    logfile=$3
    attempt=0
    while [ "$attempt" -lt 50 ]; do
        if ! kill -0 "$pid" 2>/dev/null; then
            cat "$logfile" >&2
            return 1
        fi
        if grep -q 'msg=listening' "$logfile" 2>/dev/null; then
            sleep 0.1
            if kill -0 "$pid" 2>/dev/null; then return 0; fi
            cat "$logfile" >&2
            return 1
        fi
        attempt=$((attempt + 1))
        sleep 0.1
    done
    echo "Node $addr did not become reachable; see $logfile" >&2
    return 1
}

"$bin" cohort -nodeaddr localhost:3001 -coordinator localhost:3000 -data-dir .data/demo >.data/demo/logs/cohort.log 2>&1 &
cohort_pid=$!
wait_node localhost:3001 "$cohort_pid" .data/demo/logs/cohort.log
"$bin" coordinator -nodeaddr localhost:3000 -cohorts localhost:3001 -data-dir .data/demo -viz-port 8080 >.data/demo/logs/coordinator.log 2>&1 &
coordinator_pid=$!
wait_node localhost:3000 "$coordinator_pid" .data/demo/logs/coordinator.log
"$bin" put greeting hello
"$bin" get greeting
printf '\nProtocol visualization: http://localhost:8080 (press Play)\nLogs: .data/demo/logs/\nData is preserved in .data/demo. Press Ctrl+C to stop both nodes.\n'
while kill -0 "$cohort_pid" 2>/dev/null && kill -0 "$coordinator_pid" 2>/dev/null; do sleep 1; done
echo "A demo node exited; see .data/demo/logs/." >&2
exit 1
