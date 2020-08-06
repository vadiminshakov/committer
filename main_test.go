package main

import (
	"context"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/helpers"
	"github.com/vadiminshakov/committer/peer"
	pb "github.com/vadiminshakov/committer/proto"
	"github.com/vadiminshakov/committer/server"
	"strconv"
	"testing"
	"time"
)

const (
	COORDINATOR_TYPE = "coordinator"
	FOLLOWER_TYPE    = "follower"
)

var (
	whitelist = []string{"127.0.0.1"}
	nodes     = map[string][]*config.Config{
		COORDINATOR_TYPE: {{Nodeaddr: "localhost:3000", Role: "coordinator", Followers: []string{"localhost:3001", "localhost:3002", "localhost:3003", "localhost:3004", "localhost:3005"}, Whitelist: whitelist}},
		FOLLOWER_TYPE: {
			&config.Config{Nodeaddr: "localhost:3001", Role: "follower", Coordinator: "localhost:3000", Whitelist: whitelist},
			&config.Config{Nodeaddr: "localhost:3002", Role: "follower", Coordinator: "localhost:3000", Whitelist: whitelist},
			&config.Config{Nodeaddr: "localhost:3003", Role: "follower", Coordinator: "localhost:3000", Whitelist: whitelist},
			&config.Config{Nodeaddr: "localhost:3004", Role: "follower", Coordinator: "localhost:3000", Whitelist: whitelist},
			&config.Config{Nodeaddr: "localhost:3005", Role: "follower", Coordinator: "localhost:3000", Whitelist: whitelist},
		},
	}
)

func TestMain(m *testing.M) {
	var (
		propose helpers.ProposeHook = func(req *pb.ProposeRequest) bool {
			return true
		}
		commit helpers.CommitHook = func(req *pb.CommitRequest) bool {
			return true
		}
	)

	// start followers
	for i, node := range nodes[FOLLOWER_TYPE] {
		followerServer, err := server.NewCommitServer(node.Nodeaddr,
			server.WithConfig(node), server.WithBadgerDB("/tmp/badger"+strconv.Itoa(i)),
			server.WithProposeHook(propose), server.WithCommitHook(commit))
		if err != nil {
			panic(err)
		}
		go followerServer.Run()
	}
	time.Sleep(3 * time.Second)

	// start coordinator
	coordServer, err := server.NewCommitServer(nodes[COORDINATOR_TYPE][0].Nodeaddr,
		server.WithFollowers(nodes[COORDINATOR_TYPE][0].Followers),
		server.WithConfig(nodes[COORDINATOR_TYPE][0]), server.WithBadgerDB("/tmp/badger"),
		server.WithProposeHook(propose), server.WithCommitHook(commit))
	if err != nil {
		panic(err)
	}
	go coordServer.Run()
	time.Sleep(3 * time.Second)

	m.Run()
}

func TestCommitClient_Put(t *testing.T) {
	c, err := peer.New(nodes[COORDINATOR_TYPE][0].Nodeaddr)
	if err != nil {
		t.Error(err)
	}
	resp, err := c.Put(context.Background(), "testkey", []byte("testvalue"))
	if err != nil {
		t.Error(err)
	}
	if resp.Type != pb.Type_ACK {
		t.Error("msg is not acknowledged")
	}
}
