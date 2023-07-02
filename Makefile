prepare:
	@rm -rf ./badger
	@mkdir ./badger
	@mkdir ./badger/coordinator
	@mkdir ./badger/follower

run-example-coordinator:
	@rm -rf ./badger/coordinator
	@go run . -role=coordinator -nodeaddr=localhost:3000 -followers=localhost:3001 -committype=three-phase -timeout=1000 -dbpath=./badger/coordinator -whitelist=127.0.0.1

run-example-follower:
	@rm -rf ./badger/follower
	@go run . -role=follower -coordinator=localhost:3000 -nodeaddr=localhost:3001 -committype=three-phase -timeout=1000 -dbpath=./badger/follower -whitelist=127.0.0.1

run-example-client:
	@go run ./examples/client

tests:
	@go test ./...