prepare:
	@rm -rf ./badger
	@mkdir ./badger
	@mkdir ./badger/coordinator
	@mkdir ./badger/follower

run-example-coordinator:
	@rm -rf ./badger/coordinator
	@go run . -role=coordinator -nodeaddr=localhost:3000 -followers=localhost:3001 -committype=three-phase -timeout=1000 -dbpath=./badger/coordinator -whitelist=127.0.0.1 -withtrace=true

run-example-follower:
	@rm -rf ./badger/follower
	@go run . -role=follower -coordinator=localhost:3000 -nodeaddr=localhost:3001 -committype=three-phase -timeout=1000 -dbpath=./badger/follower -whitelist=127.0.0.1 -withtrace=true

run-example-client:
	@go run ./examples/client

show-trace: start-zipkin show-trace-coordinator show-trace-follower1 show-trace-follower2 show-trace-follower3 show-trace-client

start-zipkin:
	docker rm -f zipkin
	docker run -d -p 9411:9411 --name zipkin openzipkin/zipkin
show-trace-coordinator:
	rm -rf ./badger/coordinator
	go run . -role=coordinator -nodeaddr=localhost:3000 -followers=localhost:3001,localhost:3002,localhost:3003 -committype=three-phase -timeout=1000 -dbpath=./badger/coordinator -whitelist=127.0.0.1 -withtrace=true
show-trace-follower1:
	sleep 2
	rm -rf ./badger/follower1
	go run . -role=follower -coordinator=localhost:3000 -nodeaddr=localhost:3001 -committype=three-phase -timeout=1000 -dbpath=./badger/follower1 -whitelist=127.0.0.1 -withtrace=true
show-trace-follower2:
	sleep 2
	rm -rf ./badger/follower2
	go run . -role=follower -coordinator=localhost:3000 -nodeaddr=localhost:3002 -committype=three-phase -timeout=1000 -dbpath=./badger/follower2 -whitelist=127.0.0.1 -withtrace=true
show-trace-follower3:
	sleep 2
	rm -rf ./badger/follower3
	go run . -role=follower -coordinator=localhost:3000 -nodeaddr=localhost:3003 -committype=three-phase -timeout=1000 -dbpath=./badger/follower3 -whitelist=127.0.0.1 -withtrace=true
show-trace-client:
	sleep 5
	go run ./examples/client
	echo "please open http://localhost:9411/zipkin/dependency"

tests:
	@go test ./...

upgrade:
	@sudo apt-get update
	@wget https://go.dev/dl/go1.21.0.linux-amd64.tar.gz
	@sudo tar -xvf go1.21.0.linux-amd64.tar.gz
	@sudo mv go /usr/local
	@export GOROOT=/usr/local/go
	@export GOPATH=$HOME/go
	@export PATH=$GOPATH/bin:$GOROOT/bin:$PATH
	@source ~/.profile