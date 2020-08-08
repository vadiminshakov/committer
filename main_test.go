package main

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/helpers"
	"github.com/vadiminshakov/committer/peer"
	pb "github.com/vadiminshakov/committer/proto"
	"github.com/vadiminshakov/committer/server"
	"os"
	"path"
	"testing"
	"time"
)

const (
	COORDINATOR_TYPE = "coordinator"
	FOLLOWER_TYPE    = "follower"
	BADGER_DIR       = "/tmp/badger"
)

var (
	COORDINATOR_BADGER = path.Join(BADGER_DIR, "coordinator")
	FOLLOWER_BADGER    = path.Join(BADGER_DIR, "follower")
)

var (
	whitelist = []string{"127.0.0.1"}
	nodes     = map[string][]*config.Config{
		COORDINATOR_TYPE: {
			{Nodeaddr: "localhost:3000", Role: "coordinator",
				Followers: []string{"localhost:3001", "localhost:3002", "localhost:3003", "localhost:3004", "localhost:3005"},
				Whitelist: whitelist, CommitType: "two-phase", Timeout: 1000, DBPath: path.Join(COORDINATOR_BADGER, "1")},
			{Nodeaddr: "localhost:5000", Role: "coordinator",
				Followers: []string{"localhost:3001", "localhost:3002", "localhost:3003", "localhost:3004", "localhost:3005"},
				Whitelist: whitelist, CommitType: "three-phase", Timeout: 1000, DBPath: path.Join(COORDINATOR_BADGER, "2")},
		},
		FOLLOWER_TYPE: {
			&config.Config{Nodeaddr: "localhost:3001", Role: "follower", Coordinator: "localhost:3000", Whitelist: whitelist, Timeout: 1000, DBPath: path.Join(FOLLOWER_BADGER, "1")},
			&config.Config{Nodeaddr: "localhost:3002", Role: "follower", Coordinator: "localhost:3000", Whitelist: whitelist, Timeout: 1000, DBPath: path.Join(FOLLOWER_BADGER, "2")},
			&config.Config{Nodeaddr: "localhost:3003", Role: "follower", Coordinator: "localhost:3000", Whitelist: whitelist, Timeout: 1000, DBPath: path.Join(FOLLOWER_BADGER, "3")},
			&config.Config{Nodeaddr: "localhost:3004", Role: "follower", Coordinator: "localhost:3000", Whitelist: whitelist, Timeout: 1000, DBPath: path.Join(FOLLOWER_BADGER, "4")},
			&config.Config{Nodeaddr: "localhost:3005", Role: "follower", Coordinator: "localhost:3000", Whitelist: whitelist, Timeout: 1000, DBPath: path.Join(FOLLOWER_BADGER, "5")},
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

func TestMain(m *testing.M) {
	var (
		propose helpers.ProposeHook = func(req *pb.ProposeRequest) bool {
			return true
		}
		commit helpers.CommitHook = func(req *pb.CommitRequest) bool {
			return true
		}
	)

	os.Mkdir(COORDINATOR_BADGER, os.FileMode(0777))
	os.Mkdir(FOLLOWER_BADGER, os.FileMode(0777))

	// start followers
	for _, node := range nodes[FOLLOWER_TYPE] {
		// create db dir
		os.Mkdir(node.DBPath, os.FileMode(0777))
		// start follower
		followerServer, err := server.NewCommitServer(node,
			server.WithProposeHook(propose), server.WithCommitHook(commit))
		if err != nil {
			panic(err)
		}
		go followerServer.Run()
	}
	time.Sleep(3 * time.Second)

	// start coordinators (in two- and three-phase modes)
	for _, coordConfig := range nodes[COORDINATOR_TYPE] {
		// create db dir
		os.Mkdir(coordConfig.DBPath, os.FileMode(0777))
		// start coordinator
		coordServer, err := server.NewCommitServer(coordConfig,
			server.WithProposeHook(propose), server.WithCommitHook(commit))
		if err != nil {
			panic(err)
		}
		go coordServer.Run()
	}

	time.Sleep(3 * time.Second)

	m.Run()

	// prune
	os.RemoveAll(BADGER_DIR)
}

func TestCommitClient_Put(t *testing.T) {

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
}
