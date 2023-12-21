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

start-zipkin:
	docker rm -f zipkin
	docker run -d -p 9411:9411 --name zipkin openzipkin/zipkin

tests:
	@/usr/local/go/bin/go test ./...

upgrade:
	@sudo apt-get update
	@wget https://go.dev/dl/go1.21.0.linux-amd64.tar.gz
	@sudo tar -xvf go1.21.0.linux-amd64.tar.gz
	@sudo mv go /usr/local
	@export GOROOT=/usr/local/go
	@export GOPATH=$HOME/go
	@export PATH=$GOPATH/bin:$GOROOT/bin:$PATH