package main

import (
	"context"
	"github.com/vadiminshakov/committer/io/gateway/grpc/client"
	pb "github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"github.com/vadiminshakov/committer/io/trace"
)

const addr = "localhost:3000"

func main() {
	tracer, err := trace.Tracer("client", addr)
	if err != nil {
		panic(err)
	}
	cli, err := client.New(addr, tracer)
	if err != nil {
		panic(err)
	}
	resp, err := cli.Put(context.Background(), "key3", []byte("1111"))
	if err != nil {
		panic(err)
	}
	if resp.Type != pb.Type_ACK {
		panic("msg is not acknowledged")
	}

	// read committed keys
	//key, err := cli.Get(context.Background(), "key3")
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println(string(key.Value))
}
