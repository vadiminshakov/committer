package main

import (
	"context"
	"fmt"
	"github.com/vadiminshakov/committer/io/gateway/grpc/client"
	pb "github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"github.com/vadiminshakov/committer/io/trace"
	"strconv"
)

const coordinatorAddr = "localhost:3000"

func main() {
	key, value := "somekey", "somevalue"
	tracer, err := trace.Tracer("client", coordinatorAddr)
	if err != nil {
		panic(err)
	}

	// create a client for interaction with coordinator
	cli, err := client.New(coordinatorAddr, tracer)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 5; i++ {
		// put a key-value pair
		resp, err := cli.Put(context.Background(), key+strconv.Itoa(i), []byte(value+strconv.Itoa(i)))
		if err != nil {
			panic(err)
		}
		if resp.Type != pb.Type_ACK {
			panic("msg is not acknowledged")
		}

		// read committed key
		v, err := cli.Get(context.Background(), key+strconv.Itoa(i))
		if err != nil {
			panic(err)
		}
		fmt.Printf("got value for key '%s': %s", key+strconv.Itoa(i), string(v.Value)+strconv.Itoa(i))
	}
}
