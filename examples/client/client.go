package main

import (
	"context"
	"fmt"
	"github.com/vadiminshakov/committer/io/gateway/grpc/client"
	pb "github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"github.com/vadiminshakov/committer/io/trace"
)

const coordinatorAddr = "localhost:3000"

func main() {
	key, value := "somekey", []byte("somevalue")
	tracer, err := trace.Tracer("client", coordinatorAddr)
	if err != nil {
		panic(err)
	}

	// create a client for interaction with coordinator
	cli, err := client.New(coordinatorAddr, tracer)
	if err != nil {
		panic(err)
	}

	// put a key-value pair
	resp, err := cli.Put(context.Background(), key, value)
	if err != nil {
		panic(err)
	}
	if resp.Type != pb.Type_ACK {
		panic("msg is not acknowledged")
	}

	// read committed key
	v, err := cli.Get(context.Background(), key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("got value for key '%s': %s", key, v.Value)
}
