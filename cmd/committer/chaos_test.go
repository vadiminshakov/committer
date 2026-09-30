//go:build chaos

// To run this tests you need to install toxiproxy
//
//	# macOS/Linux
//	curl -L -o toxiproxy-server https://github.com/Shopify/toxiproxy/releases/download/v2.12.0/toxiproxy-server-darwin-amd64
//	chmod +x toxiproxy-server
//	mv toxiproxy-server ~/go/bin/
//
// And then run `make test-chaos`
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

const TOXIPROXY_URL = "http://localhost:8474"

func TestChaosFollowerFailure(t *testing.T) {

	// immediate connection reset
	t.Run("immediate_reset", func(t *testing.T) {
		chaosHelper := newChaosTestHelper(TOXIPROXY_URL)
		defer chaosHelper.cleanup()

		allAddresses := make([]string, 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))
		for _, node := range nodes[COHORT_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr)
		}
		for _, node := range nodes[COORDINATOR_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr, node.ClientAddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addResetPeer(nodes[COHORT_TYPE][0].Nodeaddr, 0))

		canceller := startnodesChaos(chaosHelper, dto.ProtocolThreePhase)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].ClientAddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := cliapi.Dial(coordAddr)
		require.NoError(t, err)

		_, err = c.Commit(context.Background(), "reset_test", []byte("value"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to send propose")
	})

	// connection drops after 10 bytes of data
	t.Run("cohort failure after 10 bytes", func(t *testing.T) {
		chaosHelper := newChaosTestHelper(TOXIPROXY_URL)
		defer chaosHelper.cleanup()

		allAddresses := make([]string, 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))
		for _, node := range nodes[COHORT_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr)
		}
		for _, node := range nodes[COORDINATOR_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr, node.ClientAddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addDataLimit(nodes[COHORT_TYPE][0].Nodeaddr, 10)) // 10 bytes

		canceller := startnodesChaos(chaosHelper, dto.ProtocolThreePhase)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].ClientAddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := cliapi.Dial(coordAddr)
		require.NoError(t, err)

		_, err = c.Commit(context.Background(), "early_fail_test", []byte("value"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to send propose")
	})

	// connection drops after 50 bytes of data
	t.Run("cohort failure after 50 bytes", func(t *testing.T) {
		chaosHelper := newChaosTestHelper(TOXIPROXY_URL)
		defer chaosHelper.cleanup()

		allAddresses := make([]string, 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))
		for _, node := range nodes[COHORT_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr)
		}
		for _, node := range nodes[COORDINATOR_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr, node.ClientAddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addDataLimit(nodes[COHORT_TYPE][0].Nodeaddr, 50)) // 50 bytes

		canceller := startnodesChaos(chaosHelper, dto.ProtocolThreePhase)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].ClientAddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := cliapi.Dial(coordAddr)
		require.NoError(t, err)

		_, err = c.Commit(context.Background(), "early_fail_test", []byte("value"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to send propose")
	})

	// connection drops after 100 bytes of data
	t.Run("cohort failure after 100 bytes", func(t *testing.T) {
		chaosHelper := newChaosTestHelper(TOXIPROXY_URL)
		defer chaosHelper.cleanup()

		allAddresses := make([]string, 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))
		for _, node := range nodes[COHORT_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr)
		}
		for _, node := range nodes[COORDINATOR_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr, node.ClientAddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addDataLimit(nodes[COHORT_TYPE][0].Nodeaddr, 100)) // 100 bytes

		canceller := startnodesChaos(chaosHelper, dto.ProtocolThreePhase)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].ClientAddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := cliapi.Dial(coordAddr)
		require.NoError(t, err)

		_, err = c.Commit(context.Background(), "early_fail_test", []byte("value"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to send propose")
	})

	// connection drops after 150 bytes of data
	t.Run("cohort failure after 150 bytes", func(t *testing.T) {
		chaosHelper := newChaosTestHelper(TOXIPROXY_URL)
		defer chaosHelper.cleanup()

		allAddresses := make([]string, 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))
		for _, node := range nodes[COHORT_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr)
		}
		for _, node := range nodes[COORDINATOR_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr, node.ClientAddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addDataLimit(nodes[COHORT_TYPE][0].Nodeaddr, 150)) // 150 bytes

		canceller := startnodesChaos(chaosHelper, dto.ProtocolThreePhase)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].ClientAddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := cliapi.Dial(coordAddr)
		require.NoError(t, err)

		_, err = c.Commit(context.Background(), "early_fail_test", []byte("value"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to send precommit")
	})

	// connection drops after 200 bytes of data
	t.Run("cohort failure after 200 bytes", func(t *testing.T) {
		chaosHelper := newChaosTestHelper(TOXIPROXY_URL)
		defer chaosHelper.cleanup()

		allAddresses := make([]string, 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))
		for _, node := range nodes[COHORT_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr)
		}
		for _, node := range nodes[COORDINATOR_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr, node.ClientAddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addDataLimit(nodes[COHORT_TYPE][0].Nodeaddr, 200)) // 200 bytes

		canceller := startnodesChaos(chaosHelper, dto.ProtocolThreePhase)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].ClientAddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := cliapi.Dial(coordAddr)
		require.NoError(t, err)

		_, err = c.Commit(context.Background(), "early_fail_test", []byte("value"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to send precommit")
	})

	// connection drops after 250 bytes of data
	t.Run("cohort failure after 250 bytes", func(t *testing.T) {
		chaosHelper := newChaosTestHelper(TOXIPROXY_URL)
		defer chaosHelper.cleanup()

		allAddresses := make([]string, 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))
		for _, node := range nodes[COHORT_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr)
		}
		for _, node := range nodes[COORDINATOR_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr, node.ClientAddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addDataLimit(nodes[COHORT_TYPE][0].Nodeaddr, 250)) // 250 bytes

		canceller := startnodesChaos(chaosHelper, dto.ProtocolThreePhase)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].ClientAddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := cliapi.Dial(coordAddr)
		require.NoError(t, err)

		_, err = c.Commit(context.Background(), "commit_fail_test", []byte("test_value_250"))
		if err != nil {
			// A failure before the durable final decision is still returned to
			// the client. If a healthy cohort nevertheless applied COMMIT, all
			// healthy cohorts must converge to it.
			if valueEventuallyOnNode(nodes[COHORT_TYPE][1].ClientAddr, "commit_fail_test", []byte("test_value_250")) {
				checkValueOnCohorts(t, "commit_fail_test", []byte("test_value_250"), 0) // skip failed cohort (index 0)
				checkValueNotOnNode(t, nodes[COHORT_TYPE][0].ClientAddr, "commit_fail_test")
			}
		} else {
			// Final-decision ACKs are asynchronous: client success proves the
			// coordinator's durable/apply outcome, not immediate delivery to the
			// cohort behind the permanent fault.
			t.Log("operation committed; waiting only for reachable cohorts")
			checkValueOnCohorts(t, "commit_fail_test", []byte("test_value_250"), 0)
		}
	})

	// connection drops after 500 bytes of data
	t.Run("cohort failure after 500 bytes", func(t *testing.T) {
		chaosHelper := newChaosTestHelper(TOXIPROXY_URL)
		defer chaosHelper.cleanup()

		allAddresses := make([]string, 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))
		for _, node := range nodes[COHORT_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr)
		}
		for _, node := range nodes[COORDINATOR_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr, node.ClientAddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addDataLimit(nodes[COHORT_TYPE][0].Nodeaddr, 500)) // 500 bytes

		canceller := startnodesChaos(chaosHelper, dto.ProtocolThreePhase)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].ClientAddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := cliapi.Dial(coordAddr)
		require.NoError(t, err)

		_, err = c.Commit(context.Background(), "success_test", []byte("test_value_500"))
		require.NoError(t, err)

		// check if value was committed on all nodes
		checkValueOnAllCohorts(t, "success_test", []byte("test_value_500"))
	})
}

func TestChaosCoordinatorFailure(t *testing.T) {

	// immediate connection reset
	t.Run("coordinator_immediate_reset", func(t *testing.T) {
		chaosHelper := newChaosTestHelper(TOXIPROXY_URL)
		defer chaosHelper.cleanup()

		allAddresses := make([]string, 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))
		for _, node := range nodes[COHORT_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr)
		}
		for _, node := range nodes[COORDINATOR_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr, node.ClientAddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		for _, addr := range []string{nodes[COORDINATOR_TYPE][1].Nodeaddr, nodes[COORDINATOR_TYPE][1].ClientAddr} {
			require.NoError(t, chaosHelper.addResetPeer(addr, 0))
		}

		canceller := startnodesChaos(chaosHelper, dto.ProtocolThreePhase)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].ClientAddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := cliapi.Dial(coordAddr)
		require.NoError(t, err)

		_, err = c.Commit(context.Background(), "coord_reset_test", []byte("value"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "connection closed before server preface received")
	})

	// coordinator fails after 50 bytes
	t.Run("coordinator_failure_after_50_bytes", func(t *testing.T) {
		chaosHelper := newChaosTestHelper(TOXIPROXY_URL)
		defer chaosHelper.cleanup()

		allAddresses := make([]string, 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))
		for _, node := range nodes[COHORT_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr)
		}
		for _, node := range nodes[COORDINATOR_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr, node.ClientAddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		for _, addr := range []string{nodes[COORDINATOR_TYPE][1].Nodeaddr, nodes[COORDINATOR_TYPE][1].ClientAddr} {
			require.NoError(t, chaosHelper.addDataLimit(addr, 50))
		}

		canceller := startnodesChaos(chaosHelper, dto.ProtocolThreePhase)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].ClientAddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := cliapi.Dial(coordAddr)
		require.NoError(t, err)

		_, err = c.Commit(context.Background(), "coord_50_test", []byte("value"))
		require.Error(t, err)
	})

	// coordinator fails after 100 bytes
	t.Run("coordinator_failure_after_100_bytes", func(t *testing.T) {
		chaosHelper := newChaosTestHelper(TOXIPROXY_URL)
		defer chaosHelper.cleanup()

		allAddresses := make([]string, 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))
		for _, node := range nodes[COHORT_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr)
		}
		for _, node := range nodes[COORDINATOR_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr, node.ClientAddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		for _, addr := range []string{nodes[COORDINATOR_TYPE][1].Nodeaddr, nodes[COORDINATOR_TYPE][1].ClientAddr} {
			require.NoError(t, chaosHelper.addDataLimit(addr, 100))
		}

		canceller := startnodesChaos(chaosHelper, dto.ProtocolThreePhase)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].ClientAddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := cliapi.Dial(coordAddr)
		require.NoError(t, err)

		_, err = c.Commit(context.Background(), "coord_100_test", []byte("value"))
		require.Error(t, err)
	})

	// coordinator fails after 200 bytes
	t.Run("coordinator_failure_after_200_bytes", func(t *testing.T) {
		chaosHelper := newChaosTestHelper(TOXIPROXY_URL)
		defer chaosHelper.cleanup()

		allAddresses := make([]string, 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))
		for _, node := range nodes[COHORT_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr)
		}
		for _, node := range nodes[COORDINATOR_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr, node.ClientAddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		for _, addr := range []string{nodes[COORDINATOR_TYPE][1].Nodeaddr, nodes[COORDINATOR_TYPE][1].ClientAddr} {
			require.NoError(t, chaosHelper.addDataLimit(addr, 200))
		}

		canceller := startnodesChaos(chaosHelper, dto.ProtocolThreePhase)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].ClientAddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := cliapi.Dial(coordAddr)
		require.NoError(t, err)

		_, err = c.Commit(context.Background(), "coord_200_test", []byte("value"))
		// may succeed or fail depending on when exactly coordinator fails
		// the main point is to check cohort consistency afterwards
		if err != nil {
			t.Logf("coordinator operation failed as expected: %v", err)
		} else {
			t.Log("coordinator operation succeeded despite limits")
		}

		t.Log("checking cohort states after coordinator failure")
		checkFollowerStatesAfterCoordinatorFailure(t, "coord_200_test", []byte("value"))
	})

	// coordinator fails during commit phase (after 300 bytes)
	t.Run("coordinator_failure_during_commit", func(t *testing.T) {
		chaosHelper := newChaosTestHelper(TOXIPROXY_URL)
		defer chaosHelper.cleanup()

		allAddresses := make([]string, 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))
		for _, node := range nodes[COHORT_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr)
		}
		for _, node := range nodes[COORDINATOR_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr, node.ClientAddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		for _, addr := range []string{nodes[COORDINATOR_TYPE][1].Nodeaddr, nodes[COORDINATOR_TYPE][1].ClientAddr} {
			require.NoError(t, chaosHelper.addDataLimit(addr, 300))
		}

		canceller := startnodesChaos(chaosHelper, dto.ProtocolThreePhase)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].ClientAddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := cliapi.Dial(coordAddr)
		require.NoError(t, err)

		_, err = c.Commit(context.Background(), "coord_commit_test", []byte("commit_value"))
		// may succeed or fail depending on timing
		if err != nil {
			t.Logf("coordinator operation failed: %v", err)
		} else {
			t.Log("coordinator operation completed successfully")
		}

		t.Log("checking cohort states after coordinator failure during commit")
		checkFollowerStatesAfterCoordinatorFailure(t, "coord_commit_test", []byte("commit_value"))
	})
}

// startnodesChaos starts nodes with Toxiproxy support
func startnodesChaos(helper *chaosTestHelper, protocol dto.Protocol) func() error {
	dataDir, err := os.MkdirTemp("", "committer-chaos-")
	failfast(err)

	stopfuncs := make([]func() error, 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))

	// start cohorts
	for _, node := range nodes[COHORT_TYPE] {
		conf := *node
		conf.DataDir = dataDir
		conf.Protocol = protocol

		if protocol == dto.ProtocolThreePhase {
			// use proxy address of coordinator
			if proxyAddr := helper.getProxyAddress(nodes[COORDINATOR_TYPE][1].Nodeaddr); proxyAddr != "" {
				conf.Coordinator = proxyAddr
			} else {
				conf.Coordinator = nodes[COORDINATOR_TYPE][1].Nodeaddr
			}
		}

		stop, err := startNode(context.Background(), &conf, events.NoopEmitter{})
		failfast(err)

		stopfuncs = append(stopfuncs, stop)
	}

	// start coordinators
	for _, coordConfig := range nodes[COORDINATOR_TYPE] {
		coordinatorConfig := *coordConfig
		coordinatorConfig.DataDir = dataDir
		// update cohorts addresses to use proxies
		updatedCohorts := make([]string, len(coordConfig.Cohorts))
		for j, cohortAddr := range coordConfig.Cohorts {
			if proxyAddr := helper.getProxyAddress(cohortAddr); proxyAddr != "" {
				updatedCohorts[j] = proxyAddr
			} else {
				updatedCohorts[j] = cohortAddr
			}
		}
		coordinatorConfig.Cohorts = updatedCohorts

		stop, err := startNode(context.Background(), &coordinatorConfig, events.NoopEmitter{})
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

// valueEventuallyOnNode reports whether the node at clientAddr serves value for
// key within a short wait.
func valueEventuallyOnNode(clientAddr, key string, value []byte) bool {
	cli, err := cliapi.Dial(clientAddr)
	if err != nil {
		return false
	}
	defer cli.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got, err := cli.Get(context.Background(), key); err == nil && string(got) == string(value) {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}

	return false
}

// checkValueOnCohorts checks if a value exists on working cohorts (excluding failed one)
func checkValueOnCohorts(t *testing.T, key string, expectedValue []byte, skipFailedIndex int) int {
	t.Helper()

	successCount := 0
	for i, cohortAddr := range nodes[COHORT_TYPE] {
		if i == skipFailedIndex {
			continue
		}

		cohortClient, err := cliapi.Dial(cohortAddr.ClientAddr)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			cohortValue, err := cohortClient.Get(context.Background(), key)
			return err == nil && string(cohortValue) == string(expectedValue)
		}, 2*time.Second, 20*time.Millisecond)
		successCount++
	}
	return successCount
}

// checkValueOnAllCohorts checks if a value exists on ALL cohorts
func checkValueOnAllCohorts(t *testing.T, key string, expectedValue []byte) {
	t.Helper()

	for _, cohortAddr := range nodes[COHORT_TYPE] {
		cohortClient, err := cliapi.Dial(cohortAddr.ClientAddr)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			cohortValue, err := cohortClient.Get(context.Background(), key)
			return err == nil && string(cohortValue) == string(expectedValue)
		}, 2*time.Second, 20*time.Millisecond)
	}
}

// checkValueNotOnNode checks that a value does NOT exist on specified node
func checkValueNotOnNode(t *testing.T, nodeAddr string, key string) {
	t.Helper()

	nodeClient, err := cliapi.Dial(nodeAddr)
	require.NoError(t, err)

	_, err = nodeClient.Get(context.Background(), key)
	if err != nil {
		t.Logf("node %s correctly does not have value (as expected for failed node)", nodeAddr)
	} else {
		t.Logf("WARNING: node %s unexpectedly has the value (should not happen for failed node)", nodeAddr)
	}
}

// checkFollowerStatesAfterCoordinatorFailure checks the state of cohort nodes after coordinator failure
func checkFollowerStatesAfterCoordinatorFailure(t *testing.T, key string, expectedValue []byte) {
	t.Helper()

	committedCount := 0
	notCommittedCount := 0

	for _, cohortAddr := range nodes[COHORT_TYPE] {
		cohortClient, err := cliapi.Dial(cohortAddr.ClientAddr)
		require.NoError(t, err)

		cohortValue, err := cohortClient.Get(context.Background(), key)
		if err == nil {
			require.Equal(t, expectedValue, cohortValue)
			committedCount++
		} else {
			notCommittedCount++
		}
	}

	totalCohorts := len(nodes[COHORT_TYPE])
	t.Logf("cohort states: %d committed, %d not committed", committedCount, notCommittedCount)

	// validation: after coordinator failure, cohorts must be in consistent state
	// either all committed or none committed (no partial commits allowed)
	if committedCount > 0 && committedCount < totalCohorts {
		t.Errorf("inconsistent state detected: %d cohorts committed, %d did not commit. This violates consistency!",
			committedCount, notCommittedCount)
	} else {
		t.Logf("consistent state maintained: all cohorts are in the same state")
	}
}
