package main

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"log/slog"

	"github.com/stretchr/testify/require"
	"github.com/vadiminshakov/committer/v2/internal/config"
	"github.com/vadiminshakov/committer/v2/internal/events"
	"github.com/vadiminshakov/committer/v2/internal/io/gateway/grpc/client"
	pb "github.com/vadiminshakov/committer/v2/internal/io/gateway/grpc/proto"
)

const (
	COORDINATOR_TYPE = "coordinator"
	COHORT_TYPE      = "cohort"
)

var (
	nodes = map[string][]*config.Config{
		COORDINATOR_TYPE: {
			{Nodeaddr: "localhost:2938", Role: "coordinator",
				Cohorts:    []string{"localhost:2345", "localhost:2384", "localhost:7532", "localhost:5743", "localhost:4991"},
				CommitType: "two-phase", Timeout: 100},
			{Nodeaddr: "localhost:5002", Role: "coordinator",
				Cohorts:    []string{"localhost:2345", "localhost:2384", "localhost:7532", "localhost:5743", "localhost:4991"},
				CommitType: "three-phase", Timeout: 100},
		},
		COHORT_TYPE: {
			&config.Config{Nodeaddr: "localhost:2345", Role: "cohort", Coordinator: "localhost:2938", Timeout: 800, CommitType: "three-phase"},
			&config.Config{Nodeaddr: "localhost:2384", Role: "cohort", Coordinator: "localhost:2938", Timeout: 800, CommitType: "three-phase"},
			&config.Config{Nodeaddr: "localhost:7532", Role: "cohort", Coordinator: "localhost:2938", Timeout: 800, CommitType: "three-phase"},
			&config.Config{Nodeaddr: "localhost:5743", Role: "cohort", Coordinator: "localhost:2938", Timeout: 800, CommitType: "three-phase"},
			&config.Config{Nodeaddr: "localhost:4991", Role: "cohort", Coordinator: "localhost:2938", Timeout: 800, CommitType: "three-phase"},
		},
	}
)

var testtable = map[string][]byte{
	"key1": []byte("value1"),
	"key2": []byte("value2"),
	"key3": []byte("value3"),
}

func TestHappyPath(t *testing.T) {
	var canceller func() error

	coordConfig := nodes[COORDINATOR_TYPE][0]
	if coordConfig.CommitType == "two-phase" {
		canceller = startnodes(pb.CommitType_TWO_PHASE_COMMIT)

		slog.Info("TEST IN TWO-PHASE MODE")
	} else {
		canceller = startnodes(pb.CommitType_THREE_PHASE_COMMIT)

		slog.Info("TEST IN THREE-PHASE MODE")
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
	}

	// connect to cohorts and check that them added key-value
	for _, node := range nodes[COHORT_TYPE] {
		cli, err := client.NewClientAPI(node.Nodeaddr)
		require.NoError(t, err, "err not nil")

		for key, val := range testtable {
			require.Eventually(t, func() bool {
				resp, err := cli.Get(context.Background(), key)
				if err != nil || string(resp.Value) != string(val) {
					return false
				}

				return true
			}, 3*time.Second, 20*time.Millisecond, "cohort %s did not converge", node.Nodeaddr)
		}
	}

	require.NoError(t, canceller())
}

func startnodes(commitType pb.CommitType) func() error {
	dataDir, err := os.MkdirTemp("", "committer-test-")
	failfast(err)

	stopfuncs := make([]func() error, 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))

	// start cohorts
	for _, node := range nodes[COHORT_TYPE] {
		nodeConfig := *node
		nodeConfig.DataDir = dataDir
		nodeConfig.CommitType = config.CommitTwoPhase

		if commitType == pb.CommitType_THREE_PHASE_COMMIT {
			nodeConfig.Coordinator = nodes[COORDINATOR_TYPE][1].Nodeaddr
			nodeConfig.CommitType = config.CommitThreePhase
		}

		stop, err := startKVNode(context.Background(), &nodeConfig, events.NoopEmitter{})
		failfast(err)

		stopfuncs = append(stopfuncs, stop)
	}

	// start coordinators (in two- and three-phase modes)
	for _, coordConfig := range nodes[COORDINATOR_TYPE] {
		nodeConfig := *coordConfig
		nodeConfig.DataDir = dataDir

		stop, err := startKVNode(context.Background(), &nodeConfig, events.NoopEmitter{})
		failfast(err)

		stopfuncs = append(stopfuncs, stop)
	}

	return func() error {
		var errs error
		for _, stop := range stopfuncs {
			errs = errors.Join(errs, stop())
		}

		return errors.Join(errs, os.RemoveAll(dataDir))
	}
}

func failfast(err error) {
	if err != nil {
		panic(err)
	}
}
