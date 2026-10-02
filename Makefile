.PHONY: build tests lint lint-fix start-toxiproxy stop-toxiproxy test-chaos proto-gen generate

build:
	@go build -o bin/committer ./cmd/committer

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
	@protoc --go_out=io/gateway/grpc/proto --go_opt=paths=source_relative \
		--go-grpc_out=io/gateway/grpc/proto --go-grpc_opt=paths=source_relative \
		--proto_path=io/gateway/grpc/proto io/gateway/grpc/proto/schema.proto
	@protoc --go_out=cmd/committer/internal/cliapi/pb --go_opt=paths=source_relative \
		--go-grpc_out=cmd/committer/internal/cliapi/pb --go-grpc_opt=paths=source_relative \
		--proto_path=cmd/committer/internal/cliapi/pb cmd/committer/internal/cliapi/pb/cli.proto
	@echo "Proto files generated successfully"

generate: proto-gen
	@echo "Generating mocks..."
	@go generate ./...
	@echo "All files generated successfully"