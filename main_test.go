package main

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/hooks"
	"github.com/vadiminshakov/committer/peer"
	pb "github.com/vadiminshakov/committer/proto"
	"github.com/vadiminshakov/committer/server"
	"google.golang.org/grpc"
	"os"
	"strconv"
	"testing"
	"time"
)

const (
	COORDINATOR_TYPE = "coordinator"
	FOLLOWER_TYPE    = "follower"
	BADGER_DIR       = "/tmp/badger"
)

const (
	NOT_BLOCKING = iota
	BLOCK_ON_PRECOMMIT_FOLLOWERS
	BLOCK_ON_PRECOMMIT_COORDINATOR
)

var (
	whitelist = []string{"127.0.0.1"}
	nodes     = map[string][]*config.Config{
		COORDINATOR_TYPE: {
			{Nodeaddr: "localhost:3000", Role: "coordinator",
				Followers: []string{"localhost:3001", "localhost:3002", "localhost:3003", "localhost:3004", "localhost:3005"},
				Whitelist: whitelist, CommitType: "two-phase", Timeout: 1000, Hooks: "hooks/src/hooks.go"},
			{Nodeaddr: "localhost:5000", Role: "coordinator",
				Followers: []string{"localhost:3001", "localhost:3002", "localhost:3003", "localhost:3004", "localhost:3005"},
				Whitelist: whitelist, CommitType: "three-phase", Timeout: 1000, Hooks: "hooks/src/hooks.go"},
		},
		FOLLOWER_TYPE: {
			&config.Config{Nodeaddr: "localhost:3001", Role: "follower", Coordinator: "localhost:3000", Whitelist: whitelist, Timeout: 1000, Hooks: "hooks/src/hooks.go"},
			&config.Config{Nodeaddr: "localhost:3002", Role: "follower", Coordinator: "localhost:3000", Whitelist: whitelist, Timeout: 1000, Hooks: "hooks/src/hooks.go"},
			&config.Config{Nodeaddr: "localhost:3003", Role: "follower", Coordinator: "localhost:3000", Whitelist: whitelist, Timeout: 1000, Hooks: "hooks/src/hooks.go"},
			&config.Config{Nodeaddr: "localhost:3004", Role: "follower", Coordinator: "localhost:3000", Whitelist: whitelist, Timeout: 1000, Hooks: "hooks/src/hooks.go"},
			&config.Config{Nodeaddr: "localhost:3005", Role: "follower", Coordinator: "localhost:3000", Whitelist: whitelist, Timeout: 1000, Hooks: "hooks/src/hooks.go"},
		},
	}
)

var testtable = map[string][]byte{
	"key1": []byte("value1"),
	"key2": []byte("value2"),
	"key3": []byte("value3"),
	"key4": []byte("value4"),
	"key5": []byte("value5"),
	"key6": []byte("value6"),
	"key7": []byte("value7"),
	"key8": []byte("value8"),
}

func TestHappyPath(t *testing.T) {
	done := make(chan struct{})
	go startnodes(NOT_BLOCKING, done)
	time.Sleep(6 * time.Second) // wait for coordinators and followers to start and establish connections

	log.SetFormatter(&log.TextFormatter{
		ForceColors:     true, // Seems like automatic color detection doesn't work on windows terminals
		FullTimestamp:   true,
		TimestampFormat: time.RFC822,
	})

	for _, coordConfig := range nodes[COORDINATOR_TYPE] {
		if coordConfig.CommitType == "two-phase" {
			log.Println("***\nTEST IN TWO-PHASE MODE\n***")
		} else {
			log.Println("***\nTEST IN THREE-PHASE MODE\n***")
		}
		c, err := peer.New(coordConfig.Nodeaddr)
		if err != nil {
			t.Error(err)
		}
		for key, val := range testtable {
			resp, err := c.Put(context.Background(), key, val)
			if err != nil {
				t.Error(err)
			}
			if resp.Type != pb.Type_ACK {
				t.Error("msg is not acknowledged")
			}
		}
	}
	done <- struct{}{}
	time.Sleep(2 * time.Second)
}

// 5 followers, 1 coordinator
// on precommit stage all followers stops responding
func Test_3PC_6NODES_ALLFAILURE_ON_PRECOMMIT(t *testing.T) {

	done := make(chan struct{})
	go startnodes(BLOCK_ON_PRECOMMIT_FOLLOWERS, done)
	time.Sleep(10 * time.Second) // wait for coordinators and followers to start and establish connections

	log.SetFormatter(&log.TextFormatter{
		ForceColors:     true, // Seems like automatic color detection doesn't work on windows terminals
		FullTimestamp:   true,
		TimestampFormat: time.RFC822,
	})

	c, err := peer.New(nodes[COORDINATOR_TYPE][1].Nodeaddr)
	assert.NoError(t, err, "err not nil")
	for key, val := range testtable {
		resp, err := c.Put(context.Background(), key, val)
		assert.NoError(t, err, "err not nil")
		assert.NotEqual(t, resp.Type, pb.Type_ACK, "msg shouldn't be acknowledged")
	}

	done <- struct{}{}
	time.Sleep(2 * time.Second)
}

// 5 followers, 1 coordinator
// on precommit stage coordinator stops responding
func Test_3PC_6NODES_COORDINATORFAILURE_ON_PRECOMMIT(t *testing.T) {

	done := make(chan struct{})
	go startnodes(BLOCK_ON_PRECOMMIT_COORDINATOR, done)
	time.Sleep(10 * time.Second) // wait for coordinators and followers to start and establish connections

	log.SetFormatter(&log.TextFormatter{
		ForceColors:     true, // Seems like automatic color detection doesn't work on windows terminals
		FullTimestamp:   true,
		TimestampFormat: time.RFC822,
	})

	c, err := peer.New(nodes[COORDINATOR_TYPE][1].Nodeaddr)
	assert.NoError(t, err, "err not nil")

	for key, val := range testtable {
		resp, err := c.Put(context.Background(), key, val)
		assert.NoError(t, err, "err not nil")
		assert.Equal(t, resp.Type, pb.Type_ACK, "msg should be acknowledged")
	}

	// connect to follower and check that them added key-value
	for _, node := range nodes[FOLLOWER_TYPE] {
		cli, err := peer.New(node.Nodeaddr)
		assert.NoError(t, err, "err not nil")
		for key, val := range testtable {
			resp, err := cli.Get(context.Background(), key)
			assert.NoError(t, err, "err not nil")
			assert.Equal(t, resp.Value, val)
		}
	}

	done <- struct{}{}
	time.Sleep(2 * time.Second)
}

func startnodes(block int, done chan struct{}) {
	COORDINATOR_BADGER := fmt.Sprintf("%s%s%d", BADGER_DIR, "coordinator", time.Now().UnixNano())
	FOLLOWER_BADGER := fmt.Sprintf("%s%s%d", BADGER_DIR, "follower", time.Now().UnixNano())

	os.Mkdir(COORDINATOR_BADGER, os.FileMode(0777))
	os.Mkdir(FOLLOWER_BADGER, os.FileMode(0777))

	var blocking grpc.UnaryServerInterceptor
	switch block {
	case BLOCK_ON_PRECOMMIT_FOLLOWERS:
		blocking = server.PrecommitBlockALL
	case BLOCK_ON_PRECOMMIT_COORDINATOR:
		blocking = server.PrecommitBlockCoordinator
	}

	// start followers
	for i, node := range nodes[FOLLOWER_TYPE] {
		// create db dir
		os.Mkdir(node.DBPath, os.FileMode(0777))
		node.DBPath = fmt.Sprintf("%s%s%s", FOLLOWER_BADGER, strconv.Itoa(i), "~")
		// start follower
		hooks, err := hooks.Get(node.Hooks)
		if err != nil {
			panic(err)
		}
		followerServer, err := server.NewCommitServer(node, hooks...)
		if err != nil {
			panic(err)
		}

		if block == 0 {
			go followerServer.Run(server.WhiteListChecker)
		} else {
			go followerServer.Run(server.WhiteListChecker, blocking)
		}

		defer followerServer.Stop()
	}
	time.Sleep(3 * time.Second)

	// start coordinators (in two- and three-phase modes)
	for i, coordConfig := range nodes[COORDINATOR_TYPE] {
		// create db dir
		os.Mkdir(coordConfig.DBPath, os.FileMode(0777))
		coordConfig.DBPath = fmt.Sprintf("%s%s%s", COORDINATOR_BADGER, strconv.Itoa(i), "~")
		// start coordinator
		hooks, err := hooks.Get(coordConfig.Hooks)
		if err != nil {
			panic(err)
		}
		coordServer, err := server.NewCommitServer(coordConfig, hooks...)
		if err != nil {
			panic(err)
		}

		if block == 0 {
			go coordServer.Run(server.WhiteListChecker)
		} else {
			go coordServer.Run(server.WhiteListChecker, blocking)
		}

		defer coordServer.Stop()
	}

	<-done
	// prune
	os.RemoveAll(BADGER_DIR)
}
