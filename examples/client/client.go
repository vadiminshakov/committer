package main

import (
	"context"
	"github.com/vadiminshakov/committer/peer"
	pb "github.com/vadiminshakov/committer/proto"
	"github.com/vadiminshakov/committer/trace"
)

const addr = "localhost:3000"

func main() {
	tracer, err := trace.Tracer("client", addr)
	if err != nil {
		panic(err)
	}
	cli, err := peer.New(addr, tracer)
	if err != nil {
		panic(err)
	}
	resp, err := cli.Put(context.Background(), "1", []byte("2"))
	if err != nil {
		panic(err)
	}
	if resp.Type != pb.Type_ACK {
		panic("msg is not acknowledged")
	}
}
