prepare:
	@go build
	@cd examples/client && go build
	@mkdir /tmp/badger
	@mkdir /tmp/badger/coordinator
	@mkdir /tmp/badger/follower

run-example-coordinator:
	@./committer -withtrace=true -role=coordinator -nodeaddr=localhost:3000 -follower=localhost:3001 -committype=three-phase -timeout=1000 -dbpath=/tmp/badger/coordinator

run-example-follower:
	@./committer -withtrace=true -role=follower -nodeaddr=localhost:3001 -committype=three-phase -timeout=1000 -dbpath=/tmp/badger/follower

run-example-client:
	@examples/client/client

unit-tests:
	@cd server && go test

functional-tests:
	@go test