package client

import (
	pb "github.com/vadiminshakov/committer/proto"
	"testing"
)

const coordinator = "localhost:3000"

func TestCommitClient_Put(t *testing.T) {
	c, err := New(coordinator)
	if err != nil {
		t.Error(err)
	}
	resp, err := c.Put("testkey", []byte("testvalue"))
	if err != nil {
		t.Error(err)
	}
	if resp.Type != pb.Type_ACK {
		t.Error("msg is not acknowledged")
	}
}
