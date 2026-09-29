.PHONY: build prepare demo demo-reset run-example-coordinator run-example-cohort run-example-client tests lint lint-fix start-toxiproxy stop-toxiproxy test-chaos proto-gen generate

build:
	@go build -o bin/committer ./cmd/committer

# Compatibility target: setup never removes persisted data.
prepare:
	@mkdir -p .data/demo

demo: build
	@sh scripts/demo.sh

demo-reset:
	@rm -rf ./.data/demo
	@echo "Demo data removed."

run-example-coordinator:
	@go run ./cmd/committer coordinator -nodeaddr=localhost:3000 -cohorts=localhost:3001 -committype=two-phase -data-dir=.data/demo -viz-port=8080

run-example-cohort:
	@go run ./cmd/committer cohort -coordinator=localhost:3000 -nodeaddr=localhost:3001 -committype=two-phase -data-dir=.data/demo -viz-port=8081

run-example-client:
	@go run ./examples/client

tests:
	@go test ./...

lint:
	@golangci-lint run ./...

lint-fix:
	@golangci-lint run --fix ./...

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
	@protoc --go_out=internal/io/gateway/grpc/proto --go_opt=paths=source_relative \
		--go-grpc_out=internal/io/gateway/grpc/proto --go-grpc_opt=paths=source_relative \
		--proto_path=internal/io/gateway/grpc/proto internal/io/gateway/grpc/proto/schema.proto
	@echo "Proto files generated successfully"

generate: proto-gen
	@echo "Generating mocks..."
	@go generate ./...
	@echo "All files generated successfully"