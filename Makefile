prepare:
	@rm -rf ./badger
	@mkdir ./badger
	@mkdir ./badger/coordinator
	@mkdir ./badger/cohort

run-example-coordinator:
	@rm -rf ./badger/coordinator
	@go run . -role=coordinator -nodeaddr=localhost:3000 -cohorts=localhost:3001 -committype=three-phase -timeout=1000 -dbpath=./badger/coordinator -whitelist=127.0.0.1

run-example-cohort:
	@rm -rf ./badger/cohort
	@go run . -role=cohort -coordinator=localhost:3000 -nodeaddr=localhost:3001 -committype=three-phase -timeout=1000 -dbpath=./badger/cohort -whitelist=127.0.0.1

run-example-client:
	@go run ./examples/client

tests:
	@go test ./...

start-toxiproxy:
	@echo "Starting Toxiproxy server..."
	@pkill toxiproxy-server || true
	@toxiproxy-server > /dev/null 2>&1 &
	@echo "Toxiproxy server started in background"

stop-toxiproxy:
	@echo "Stopping Toxiproxy server..."
	@pkill toxiproxy-server || true

test-chaos: start-toxiproxy
	@echo "Waiting for Toxiproxy to start..."
	@sleep 2
	@echo "Running chaos tests..."
	@go test -v -tags=chaos -run "TestChaos" ./...
	@$(MAKE) stop-toxiproxy

proto-gen:
	@echo "Generating proto files..."
	@protoc --go_out=io/gateway/grpc/proto --go_opt=paths=source_relative \
		--go-grpc_out=io/gateway/grpc/proto --go-grpc_opt=paths=source_relative \
		--proto_path=io/gateway/grpc/proto io/gateway/grpc/proto/schema.proto
	@echo "Proto files generated successfully"

generate: proto-gen
	@echo "Generating mocks..."
	@go generate ./...
	@echo "All files generated successfully"