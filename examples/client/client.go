package main

import (
	"context"
	"github.com/vadiminshakov/committer/peer"
	pb "github.com/vadiminshakov/committer/proto"
)

func main() {
	cli, err := peer.New("localhost:3000")
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
