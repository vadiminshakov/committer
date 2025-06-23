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
	@/usr/local/go/bin/go test ./...

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

upgrade:
	@sudo apt-get update
	@wget https://go.dev/dl/go1.21.0.linux-amd64.tar.gz
	@sudo tar -xvf go1.21.0.linux-amd64.tar.gz
	@sudo mv go /usr/local
	@export GOROOT=/usr/local/go
	@export GOPATH=$HOME/go
	@export PATH=$GOPATH/bin:$GOROOT/bin:$PATH