package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/cohort"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo/hooks"
	"github.com/vadiminshakov/committer/core/coordinator"
	"github.com/vadiminshakov/committer/io/db"
	"github.com/vadiminshakov/committer/io/gateway/grpc/client"
	pb "github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
	"github.com/vadiminshakov/gowal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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
	ONE_FOLLOWER_FAIL
)

var (
	whitelist = []string{"127.0.0.1"}
	nodes     = map[string][]*config.Config{
		COORDINATOR_TYPE: {
			{Nodeaddr: "localhost:2938", Role: "coordinator",
				Followers: []string{"localhost:2345", "localhost:2384", "localhost:7532", "localhost:5743", "localhost:4991"},
				Whitelist: whitelist, CommitType: "two-phase", Timeout: 100},
			{Nodeaddr: "localhost:5002", Role: "coordinator",
				Followers: []string{"localhost:2345", "localhost:2384", "localhost:7532", "localhost:5743", "localhost:4991"},
				Whitelist: whitelist, CommitType: "three-phase", Timeout: 100},
		},
		FOLLOWER_TYPE: {
			&config.Config{Nodeaddr: "localhost:2345", Role: "follower", Coordinator: "localhost:2938", Whitelist: whitelist, Timeout: 800, CommitType: "three-phase"},
			&config.Config{Nodeaddr: "localhost:2384", Role: "follower", Coordinator: "localhost:2938", Whitelist: whitelist, Timeout: 800, CommitType: "three-phase"},
			&config.Config{Nodeaddr: "localhost:7532", Role: "follower", Coordinator: "localhost:2938", Whitelist: whitelist, Timeout: 800, CommitType: "three-phase"},
			&config.Config{Nodeaddr: "localhost:5743", Role: "follower", Coordinator: "localhost:2938", Whitelist: whitelist, Timeout: 800, CommitType: "three-phase"},
			&config.Config{Nodeaddr: "localhost:4991", Role: "follower", Coordinator: "localhost:2938", Whitelist: whitelist, Timeout: 800, CommitType: "three-phase"},
		},
	}
)

var testtable = map[string][]byte{
	"key1": []byte("value1"),
	"key2": []byte("value2"),
	"key3": []byte("value3"),
}

func TestHappyPath(t *testing.T) {
	log.SetLevel(log.InfoLevel)

	var canceller func() error

	var height uint64 = 0
	coordConfig := nodes[COORDINATOR_TYPE][0]
	if coordConfig.CommitType == "two-phase" {
		canceller = startnodes(NOT_BLOCKING, pb.CommitType_TWO_PHASE_COMMIT)
		log.Println("***\nTEST IN TWO-PHASE MODE\n***")
	} else {
		canceller = startnodes(NOT_BLOCKING, pb.CommitType_THREE_PHASE_COMMIT)
		log.Println("***\nTEST IN THREE-PHASE MODE\n***")
	}

	defer canceller()

	c, err := client.NewClientAPI(coordConfig.Nodeaddr)
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
		// ok, value is added, let's increment height counter
		height++
	}

	// wait for rollback on followers
	time.Sleep(1 * time.Second)

	// connect to followers and check that them added key-value
	for _, node := range nodes[FOLLOWER_TYPE] {
		cli, err := client.NewClientAPI(node.Nodeaddr)
		require.NoError(t, err, "err not nil")

		for key, val := range testtable {
			// check values added by nodes
			resp, err := cli.Get(context.Background(), key)
			require.NoError(t, err, "err not nil")
			require.Equal(t, resp.Value, val)

			// check height of node
			nodeInfo, err := cli.NodeInfo(context.Background())
			require.NoError(t, err, "err not nil")
			require.Equal(t, height, nodeInfo.Height, "node %s ahead, %d commits behind (current height is %d)", node.Nodeaddr, nodeInfo.Height-height, nodeInfo.Height)
		}
	}

	require.NoError(t, canceller())
}

// 5 followers, 1 coordinator
// on precommit stage all followers stops responding
//
// result: coordinator waits for followers up and continues voting.
func Test_3PC_6NODES_ALLFAILURE_ON_PRECOMMIT(t *testing.T) {
	log.SetLevel(log.FatalLevel)

	canceller := startnodes(BLOCK_ON_PRECOMMIT_FOLLOWERS, pb.CommitType_THREE_PHASE_COMMIT)
	defer canceller()

	c, err := client.NewClientAPI(nodes[COORDINATOR_TYPE][1].Nodeaddr)
	require.NoError(t, err, "err not nil")
	for key, val := range testtable {
		resp, err := c.Put(context.Background(), key, val)
		require.NoError(t, err, "err not nil")
		require.Equal(t, resp.Type, pb.Type_ACK, "msg shouldn't be acknowledged")
	}

	require.NoError(t, canceller())
}

// 5 followers, 1 coordinator
// on precommit stage coordinator stops responding
//
// result: followers wait for specified timeout, and then make autocommit without coordinator.
func Test_3PC_6NODES_COORDINATOR_FAILURE_ON_PRECOMMIT_OK(t *testing.T) {
	log.SetLevel(log.InfoLevel)

	canceller := startnodes(BLOCK_ON_PRECOMMIT_COORDINATOR, pb.CommitType_THREE_PHASE_COMMIT)
	defer canceller()

	c, err := client.NewClientAPI(nodes[COORDINATOR_TYPE][1].Nodeaddr)
	require.NoError(t, err, "err not nil")

	var height uint64 = 0
	for key, val := range testtable {
		resp, err := c.Put(context.Background(), key, val)
		require.NoError(t, err, "err not nil")
		require.Equal(t, resp.Type, pb.Type_NACK, "msg should not be acknowledged")
		height += 1
	}

	// connect to followers and check that them added key-value
	for _, node := range nodes[FOLLOWER_TYPE] {
		cli, err := client.NewClientAPI(node.Nodeaddr)
		require.NoError(t, err, "err not nil")
		for key, val := range testtable {
			// check values added by nodes
			resp, err := cli.Get(context.Background(), key)
			require.NoError(t, err, "err not nil")
			require.Equal(t, resp.Value, val)

			// check height of node
			nodeInfo, err := cli.NodeInfo(context.Background())
			require.NoError(t, err, "err not nil")
			require.Equal(t, int(height), int(nodeInfo.Height), "node %s ahead, %d commits behind (current height is %d)", node.Nodeaddr, height-nodeInfo.Height, nodeInfo.Height)
		}
	}

	require.NoError(t, canceller())
}

// 5 followers, 1 coordinator
// 4 followers acked msg, 1 failed.
//
// result: msg proposed, then rollback.
func Test_3PC_6NODES_COORDINATOR_FAILURE_ON_PRECOMMIT_ONE_FOLLOWER_FAILED(t *testing.T) {
	log.SetLevel(log.FatalLevel)

	canceller := startnodes(ONE_FOLLOWER_FAIL, pb.CommitType_THREE_PHASE_COMMIT)
	defer canceller()

	c, err := client.NewClientAPI(nodes[COORDINATOR_TYPE][1].Nodeaddr)
	require.NoError(t, err, "err not nil")

	for key, val := range testtable {
		md := metadata.Pairs("blockcommit", "1000ms")
		ctx := metadata.NewOutgoingContext(context.Background(), md)
		if _, err = c.Put(ctx, key, val); err != nil {
			break
		}
	}

	time.Sleep(9 * time.Second)

	// connect to follower and check that them NOT added key-value
	for _, node := range nodes[FOLLOWER_TYPE] {
		cli, err := client.NewClientAPI(node.Nodeaddr)
		require.NoError(t, err, "err not nil")
		for key := range testtable {
			// check values NOT added by nodes
			resp, err := cli.Get(context.Background(), key)
			require.Error(t, err, "expected an error but got nil")
			require.Contains(t, err.Error(), "Key not found")
			require.Equal(t, (*pb.Value)(nil), resp)

			// check height of node
			nodeInfo, err := cli.NodeInfo(context.Background())
			require.NoError(t, err, "err not nil")
			require.EqualValues(t, 0, nodeInfo.Height, "node %s must have 0 height (but has %d)", node.Nodeaddr, nodeInfo.Height)
		}
	}

	require.NoError(t, canceller())
}

func startnodes(block int, commitType pb.CommitType) func() error {
	COORDINATOR_BADGER := fmt.Sprintf("%s%s%d", BADGER_DIR, "coordinator", time.Now().UnixNano())
	FOLLOWER_BADGER := fmt.Sprintf("%s%s%d", BADGER_DIR, "follower", time.Now().UnixNano())

	// check dir exists
	if _, err := os.Stat(COORDINATOR_BADGER); !os.IsNotExist(err) {
		// del dir
		err := os.RemoveAll(COORDINATOR_BADGER)
		failfast(err)
	}
	if _, err := os.Stat(FOLLOWER_BADGER); !os.IsNotExist(err) {
		// del dir
		failfast(os.RemoveAll(FOLLOWER_BADGER))
	}
	if _, err := os.Stat("./tmp"); !os.IsNotExist(err) {
		// del dir
		failfast(os.RemoveAll("./tmp"))

	}

	{
		// nolint:errcheck
		failfast(os.Mkdir(COORDINATOR_BADGER, os.FileMode(0777)))
		failfast(os.Mkdir(FOLLOWER_BADGER, os.FileMode(0777)))
		failfast(os.Mkdir("./tmp", os.FileMode(0777)))
		failfast(os.Mkdir("./tmp/cohort", os.FileMode(0777)))
		failfast(os.Mkdir("./tmp/coord", os.FileMode(0777)))
	}

	var blocking grpc.UnaryServerInterceptor
	switch block {
	case BLOCK_ON_PRECOMMIT_FOLLOWERS:
		blocking = server.PrecommitBlockALL
	case BLOCK_ON_PRECOMMIT_COORDINATOR:
		blocking = server.PrecommitBlockCoordinator
	case ONE_FOLLOWER_FAIL:
		blocking = server.ProposeOneFollowerFail
	}

	// start followers
	stopfuncs := make([]func(), 0, len(nodes[FOLLOWER_TYPE])+len(nodes[COORDINATOR_TYPE]))
	for i, node := range nodes[FOLLOWER_TYPE] {
		if commitType == pb.CommitType_THREE_PHASE_COMMIT {
			node.Coordinator = nodes[COORDINATOR_TYPE][1].Nodeaddr
		}
		// create db dir
		node.DBPath = fmt.Sprintf("%s%s%s", FOLLOWER_BADGER, strconv.Itoa(i), "~")
		failfast(os.Mkdir(node.DBPath, os.FileMode(0777)))
		// start follower
		database, err := db.New(node.DBPath)
		failfast(err)

		walConfig := gowal.Config{
			Dir:              "./tmp/cohort/" + strconv.Itoa(i),
			Prefix:           "msgs_",
			SegmentThreshold: 100,
			MaxSegments:      100,
			IsInSyncDiskMode: false,
		}
		c, err := gowal.NewWAL(walConfig)
		failfast(err)

		ct := server.TWO_PHASE
		if commitType == pb.CommitType_THREE_PHASE_COMMIT {
			ct = server.THREE_PHASE
		}

		committer := commitalgo.NewCommitter(database, ct, c, hooks.Propose, hooks.Commit, node.Timeout)
		cohortImpl := cohort.NewCohort(committer, cohort.Mode(node.CommitType))

		followerServer, err := server.New(node, cohortImpl, nil, database)
		failfast(err)

		if block == 0 {
			go followerServer.Run(server.WhiteListChecker)
		} else {
			go followerServer.Run(server.WhiteListChecker, blocking)
		}

		stopfuncs = append(stopfuncs, followerServer.Stop)
	}

	// start coordinators (in two- and three-phase modes)
	for i, coordConfig := range nodes[COORDINATOR_TYPE] {
		// create db dir
		coordConfig.DBPath = fmt.Sprintf("%s%s%s", COORDINATOR_BADGER, strconv.Itoa(i), "~")
		failfast(os.Mkdir(coordConfig.DBPath, os.FileMode(0777)))

		// start coordinator
		database, err := db.New(coordConfig.DBPath)
		failfast(err)

		walConfig := gowal.Config{
			Dir:              "./tmp/coord/msgs" + strconv.Itoa(i),
			Prefix:           "msgs",
			SegmentThreshold: 100,
			MaxSegments:      100,
			IsInSyncDiskMode: false,
		}

		c, err := gowal.NewWAL(walConfig)
		failfast(err)

		coord, err := coordinator.New(coordConfig, c, database)
		failfast(err)

		coordServer, err := server.New(coordConfig, nil, coord, database)
		failfast(err)

		if block == 0 {
			go coordServer.Run(server.WhiteListChecker)
		} else {
			go coordServer.Run(server.WhiteListChecker, blocking)
		}
		time.Sleep(100 * time.Millisecond)
		stopfuncs = append(stopfuncs, coordServer.Stop)
	}

	return func() error {
		for _, f := range stopfuncs {
			f()
		}
		failfast(os.RemoveAll("./tmp"))
		return os.RemoveAll(BADGER_DIR)
	}
}

func failfast(err error) {
	if err != nil {
		panic(err)
	}
}
