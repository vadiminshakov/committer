.PHONY: build tests lint lint-fix proto-gen generate

build:
	@go build -o bin/committer ./cmd/committer

tests:
	@go test ./...

lint:
	@golangci-lint run ./...

lint-fix:
	@golangci-lint run --fix ./...

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