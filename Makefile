prepare:
	@go build
	@cd examples/client && go build
	@mkdir /tmp/badger
	@mkdir /tmp/badger/coordinator
	@mkdir /tmp/badger/follower

run-example-coordinator:
	@./committer -role=coordinator -nodeaddr=localhost:3000 -follower=localhost:3001 -committype=three-phase -timeout=1000 -dbpath=/tmp/badger/coordinator

run-example-follower:
	@./committer -role=follower -nodeaddr=localhost:3001 -committype=three-phase -timeout=1000 -dbpath=/tmp/badger/follower

run-example-client:
	@examples/client/client

unit-tests:
	@cd core && go test

functional-tests:
	@go test