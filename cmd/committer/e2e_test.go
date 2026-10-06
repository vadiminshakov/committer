package main

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vadiminshakov/committer/v2/cmd/committer/internal/cliapi"
	"github.com/vadiminshakov/committer/v2/core/dto"
	"github.com/vadiminshakov/committer/v2/events"
)

const (
	COORDINATOR_TYPE = "coordinator"
	COHORT_TYPE      = "cohort"
)

var (
	nodes = map[string][]*nodeConfig{
		COORDINATOR_TYPE: {
			{Nodeaddr: "localhost:2938", ClientAddr: "localhost:3938", Role: "coordinator",
				Cohorts:  []string{"localhost:2345", "localhost:2384", "localhost:7532", "localhost:5743", "localhost:4991"},
				Protocol: dto.ProtocolTwoPhase, Timeout: 100 * time.Millisecond},
			{Nodeaddr: "localhost:5002", ClientAddr: "localhost:6002", Role: "coordinator",
				Cohorts:  []string{"localhost:2345", "localhost:2384", "localhost:7532", "localhost:5743", "localhost:4991"},
				Protocol: dto.ProtocolThreePhase, Timeout: 100 * time.Millisecond},
		},
		COHORT_TYPE: {
			&nodeConfig{Nodeaddr: "localhost:2345", ClientAddr: "localhost:3345", Role: "cohort", Coordinator: "localhost:2938", Timeout: 800 * time.Millisecond, Protocol: dto.ProtocolThreePhase},
			&nodeConfig{Nodeaddr: "localhost:2384", ClientAddr: "localhost:3384", Role: "cohort", Coordinator: "localhost:2938", Timeout: 800 * time.Millisecond, Protocol: dto.ProtocolThreePhase},
			&nodeConfig{Nodeaddr: "localhost:7532", ClientAddr: "localhost:8532", Role: "cohort", Coordinator: "localhost:2938", Timeout: 800 * time.Millisecond, Protocol: dto.ProtocolThreePhase},
			&nodeConfig{Nodeaddr: "localhost:5743", ClientAddr: "localhost:6743", Role: "cohort", Coordinator: "localhost:2938", Timeout: 800 * time.Millisecond, Protocol: dto.ProtocolThreePhase},
			&nodeConfig{Nodeaddr: "localhost:4991", ClientAddr: "localhost:5991", Role: "cohort", Coordinator: "localhost:2938", Timeout: 800 * time.Millisecond, Protocol: dto.ProtocolThreePhase},
		},
	}
)

var testtable = map[string][]byte{
	"key1": []byte("value1"),
	"key2": []byte("value2"),
	"key3": []byte("value3"),
}

func TestHappyPath(t *testing.T) {
	for _, protocol := range []dto.Protocol{dto.ProtocolTwoPhase, dto.ProtocolThreePhase} {
		t.Run(protocol.String(), func(t *testing.T) {
			testHappyPath(t, protocol)
		})
	}
}

func testHappyPath(t *testing.T, protocol dto.Protocol) {
	canceller := startnodes(protocol)

	t.Cleanup(func() {
		require.NoError(t, canceller())
	})

	coordAddr := nodes[COORDINATOR_TYPE][0].ClientAddr
	if protocol == dto.ProtocolThreePhase {
		coordAddr = nodes[COORDINATOR_TYPE][1].ClientAddr
	}

	c, err := cliapi.Dial(coordAddr)
	require.NoError(t, err)

	for key, val := range testtable {
		_, err := c.Commit(context.Background(), key, val)
		require.NoError(t, err)
	}

	// connect to cohorts and check that them added key-value
	for _, node := range nodes[COHORT_TYPE] {
		cli, err := cliapi.Dial(node.ClientAddr)
		require.NoError(t, err, "err not nil")

		for key, val := range testtable {
			require.Eventually(t, func() bool {
				value, err := cli.Get(context.Background(), key)
				if err != nil || string(value) != string(val) {
					return false
				}

				return true
			}, 3*time.Second, 20*time.Millisecond, "cohort %s did not converge", node.Nodeaddr)
		}
	}
}

func startnodes(protocol dto.Protocol) func() error {
	dataDir, err := os.MkdirTemp("", "committer-test-")
	failfast(err)

	stopfuncs := make([]func() error, 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))

	// start cohorts
	for _, node := range nodes[COHORT_TYPE] {
		conf := *node
		conf.DataDir = dataDir
		conf.Protocol = protocol

		if protocol == dto.ProtocolThreePhase {
			conf.Coordinator = nodes[COORDINATOR_TYPE][1].Nodeaddr
		}

		stop, err := startNode(context.Background(), &conf, events.NoopEmitter{})
		failfast(err)

		stopfuncs = append(stopfuncs, stop)
	}

	// start coordinators (in two- and three-phase modes)
	for _, coordConfig := range nodes[COORDINATOR_TYPE] {
		conf := *coordConfig
		conf.DataDir = dataDir

		stop, err := startNode(context.Background(), &conf, events.NoopEmitter{})
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
