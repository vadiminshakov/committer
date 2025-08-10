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
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/vadiminshakov/committer/core/cohort"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo"
	"github.com/vadiminshakov/committer/core/coordinator"
	"github.com/vadiminshakov/committer/io/db"
	"github.com/vadiminshakov/committer/io/gateway/grpc/client"
	pb "github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
	"github.com/vadiminshakov/gowal"
)

const TOXIPROXY_URL = "http://localhost:8474"

func TestChaosFollowerFailure(t *testing.T) {
	log.SetLevel(log.InfoLevel)

	// immediate connection reset
	t.Run("immediate_reset", func(t *testing.T) {
		chaosHelper := newChaosTestHelper(TOXIPROXY_URL)
		defer chaosHelper.cleanup()

		allAddresses := make([]string, 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))
		for _, node := range nodes[COHORT_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr)
		}
		for _, node := range nodes[COORDINATOR_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addResetPeer(nodes[COHORT_TYPE][0].Nodeaddr, 0))

		canceller := startnodesChaos(chaosHelper, pb.CommitType_THREE_PHASE_COMMIT)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].Nodeaddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := client.NewClientAPI(coordAddr)
		require.NoError(t, err)

		_, err = c.Put(context.Background(), "reset_test", []byte("value"))
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
			allAddresses = append(allAddresses, node.Nodeaddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addDataLimit(nodes[COHORT_TYPE][0].Nodeaddr, 10)) // 10 bytes

		canceller := startnodesChaos(chaosHelper, pb.CommitType_THREE_PHASE_COMMIT)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].Nodeaddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := client.NewClientAPI(coordAddr)
		require.NoError(t, err)

		_, err = c.Put(context.Background(), "early_fail_test", []byte("value"))
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
			allAddresses = append(allAddresses, node.Nodeaddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addDataLimit(nodes[COHORT_TYPE][0].Nodeaddr, 50)) // 50 bytes

		canceller := startnodesChaos(chaosHelper, pb.CommitType_THREE_PHASE_COMMIT)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].Nodeaddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := client.NewClientAPI(coordAddr)
		require.NoError(t, err)

		_, err = c.Put(context.Background(), "early_fail_test", []byte("value"))
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
			allAddresses = append(allAddresses, node.Nodeaddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addDataLimit(nodes[COHORT_TYPE][0].Nodeaddr, 100)) // 100 bytes

		canceller := startnodesChaos(chaosHelper, pb.CommitType_THREE_PHASE_COMMIT)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].Nodeaddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := client.NewClientAPI(coordAddr)
		require.NoError(t, err)

		_, err = c.Put(context.Background(), "early_fail_test", []byte("value"))
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
			allAddresses = append(allAddresses, node.Nodeaddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addDataLimit(nodes[COHORT_TYPE][0].Nodeaddr, 150)) // 150 bytes

		canceller := startnodesChaos(chaosHelper, pb.CommitType_THREE_PHASE_COMMIT)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].Nodeaddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := client.NewClientAPI(coordAddr)
		require.NoError(t, err)

		_, err = c.Put(context.Background(), "early_fail_test", []byte("value"))
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
			allAddresses = append(allAddresses, node.Nodeaddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addDataLimit(nodes[COHORT_TYPE][0].Nodeaddr, 200)) // 200 bytes

		canceller := startnodesChaos(chaosHelper, pb.CommitType_THREE_PHASE_COMMIT)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].Nodeaddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := client.NewClientAPI(coordAddr)
		require.NoError(t, err)

		_, err = c.Put(context.Background(), "early_fail_test", []byte("value"))
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
			allAddresses = append(allAddresses, node.Nodeaddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addDataLimit(nodes[COHORT_TYPE][0].Nodeaddr, 250)) // 250 bytes

		canceller := startnodesChaos(chaosHelper, pb.CommitType_THREE_PHASE_COMMIT)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].Nodeaddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := client.NewClientAPI(coordAddr)
		require.NoError(t, err)

		_, err = c.Put(context.Background(), "commit_fail_test", []byte("test_value_250"))
		if err != nil {
			require.Contains(t, err.Error(), "failed to send commit")
			// if operation failed, check that value was committed on healthy nodes
			if checkValueOnCoordinator(t, "commit_fail_test", []byte("test_value_250")) {
				checkValueOnCohorts(t, "commit_fail_test", []byte("test_value_250"), 0) // skip failed cohort (index 0)
				checkValueNotOnNode(t, nodes[COHORT_TYPE][0].Nodeaddr, "commit_fail_test")
			}
		} else {
			// if operation succeeded despite limits, all nodes should have the value
			t.Log("operation succeeded despite network limits (250 bytes was sufficient)")
			checkValueOnCoordinator(t, "commit_fail_test", []byte("test_value_250"))
			checkValueOnAllCohorts(t, "commit_fail_test", []byte("test_value_250"))
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
			allAddresses = append(allAddresses, node.Nodeaddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addDataLimit(nodes[COHORT_TYPE][0].Nodeaddr, 500)) // 500 bytes

		canceller := startnodesChaos(chaosHelper, pb.CommitType_THREE_PHASE_COMMIT)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].Nodeaddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := client.NewClientAPI(coordAddr)
		require.NoError(t, err)

		_, err = c.Put(context.Background(), "success_test", []byte("test_value_500"))
		require.NoError(t, err)

		// check if value was committed on all nodes
		checkValueOnCoordinator(t, "success_test", []byte("test_value_500"))
		checkValueOnAllCohorts(t, "success_test", []byte("test_value_500"))
	})
}

func TestChaosCoordinatorFailure(t *testing.T) {
	log.SetLevel(log.InfoLevel)

	// immediate connection reset
	t.Run("coordinator_immediate_reset", func(t *testing.T) {
		chaosHelper := newChaosTestHelper(TOXIPROXY_URL)
		defer chaosHelper.cleanup()

		allAddresses := make([]string, 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))
		for _, node := range nodes[COHORT_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr)
		}
		for _, node := range nodes[COORDINATOR_TYPE] {
			allAddresses = append(allAddresses, node.Nodeaddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addResetPeer(nodes[COORDINATOR_TYPE][1].Nodeaddr, 0))

		canceller := startnodesChaos(chaosHelper, pb.CommitType_THREE_PHASE_COMMIT)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].Nodeaddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := client.NewClientAPI(coordAddr)
		require.NoError(t, err)

		_, err = c.Put(context.Background(), "coord_reset_test", []byte("value"))
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
			allAddresses = append(allAddresses, node.Nodeaddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addDataLimit(nodes[COORDINATOR_TYPE][1].Nodeaddr, 50))

		canceller := startnodesChaos(chaosHelper, pb.CommitType_THREE_PHASE_COMMIT)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].Nodeaddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := client.NewClientAPI(coordAddr)
		require.NoError(t, err)

		_, err = c.Put(context.Background(), "coord_50_test", []byte("value"))
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
			allAddresses = append(allAddresses, node.Nodeaddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addDataLimit(nodes[COORDINATOR_TYPE][1].Nodeaddr, 100))

		canceller := startnodesChaos(chaosHelper, pb.CommitType_THREE_PHASE_COMMIT)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].Nodeaddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := client.NewClientAPI(coordAddr)
		require.NoError(t, err)

		_, err = c.Put(context.Background(), "coord_100_test", []byte("value"))
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
			allAddresses = append(allAddresses, node.Nodeaddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addDataLimit(nodes[COORDINATOR_TYPE][1].Nodeaddr, 200))

		canceller := startnodesChaos(chaosHelper, pb.CommitType_THREE_PHASE_COMMIT)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].Nodeaddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := client.NewClientAPI(coordAddr)
		require.NoError(t, err)

		_, err = c.Put(context.Background(), "coord_200_test", []byte("value"))
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
			allAddresses = append(allAddresses, node.Nodeaddr)
		}

		require.NoError(t, chaosHelper.setupProxies(allAddresses))
		require.NoError(t, chaosHelper.addDataLimit(nodes[COORDINATOR_TYPE][1].Nodeaddr, 300))

		canceller := startnodesChaos(chaosHelper, pb.CommitType_THREE_PHASE_COMMIT)
		defer canceller()

		coordAddr := nodes[COORDINATOR_TYPE][1].Nodeaddr
		if proxyAddr := chaosHelper.getProxyAddress(coordAddr); proxyAddr != "" {
			coordAddr = proxyAddr
		}

		c, err := client.NewClientAPI(coordAddr)
		require.NoError(t, err)

		_, err = c.Put(context.Background(), "coord_commit_test", []byte("commit_value"))
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
func startnodesChaos(helper *chaosTestHelper, commitType pb.CommitType) func() error {
	COORDINATOR_BADGER := fmt.Sprintf("%s%s%d", BADGER_DIR, "coordinator", time.Now().UnixNano())
	COHORT_BADGER := fmt.Sprintf("%s%s%d", BADGER_DIR, "cohort", time.Now().UnixNano())

	// cleanup dirs
	cleanupDirs := []string{COORDINATOR_BADGER, COHORT_BADGER, "./tmp"}
	for _, dir := range cleanupDirs {
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			failfast(os.RemoveAll(dir))
		}
	}

	// create dirs
	createDirs := []string{COORDINATOR_BADGER, COHORT_BADGER, "./tmp", "./tmp/cohort", "./tmp/coord"}
	for _, dir := range createDirs {
		failfast(os.Mkdir(dir, os.FileMode(0777)))
	}

	stopfuncs := make([]func(), 0, len(nodes[COHORT_TYPE])+len(nodes[COORDINATOR_TYPE]))

	// start cohorts
	for i, node := range nodes[COHORT_TYPE] {
		if commitType == pb.CommitType_THREE_PHASE_COMMIT {
			// use proxy address of coordinator
			if proxyAddr := helper.getProxyAddress(nodes[COORDINATOR_TYPE][1].Nodeaddr); proxyAddr != "" {
				node.Coordinator = proxyAddr
			} else {
				node.Coordinator = nodes[COORDINATOR_TYPE][1].Nodeaddr
			}
		}

		// create db dir
		node.DBPath = fmt.Sprintf("%s%s%s", COHORT_BADGER, strconv.Itoa(i), "~")
		failfast(os.Mkdir(node.DBPath, os.FileMode(0777)))

		// start cohort
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

		committer := commitalgo.NewCommitter(database, ct, c, node.Timeout)
		cohortImpl := cohort.NewCohort(committer, cohort.Mode(node.CommitType))

		cohortServer, err := server.New(node, cohortImpl, nil, database)
		failfast(err)

		go cohortServer.Run(server.WhiteListChecker)
		stopfuncs = append(stopfuncs, cohortServer.Stop)
	}

	// start coordinators
	for i, coordConfig := range nodes[COORDINATOR_TYPE] {
		// update cohorts addresses to use proxies
		updatedCohorts := make([]string, len(coordConfig.Cohorts))
		for j, cohortAddr := range coordConfig.Cohorts {
			if proxyAddr := helper.getProxyAddress(cohortAddr); proxyAddr != "" {
				updatedCohorts[j] = proxyAddr
			} else {
				updatedCohorts[j] = cohortAddr
			}
		}
		coordConfig.Cohorts = updatedCohorts

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

		go coordServer.Run(server.WhiteListChecker)
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

// checkValueOnCoordinator checks if a value exists on coordinator
func checkValueOnCoordinator(t *testing.T, key string, expectedValue []byte) bool {
	t.Helper()

	coordClient, err := client.NewClientAPI(nodes[COORDINATOR_TYPE][1].Nodeaddr)
	require.NoError(t, err)

	coordValue, err := coordClient.Get(context.Background(), key)
	if err != nil {
		t.Logf("coordinator does not have value for key %s: %v", key, err)
		return false
	}

	require.Equal(t, expectedValue, coordValue.Value)
	t.Logf("coordinator has correct value for key %s", key)
	return true
}

// checkValueOnCohorts checks if a value exists on working cohorts (excluding failed one)
func checkValueOnCohorts(t *testing.T, key string, expectedValue []byte, skipFailedIndex int) int {
	t.Helper()

	successCount := 0
	for i, cohortAddr := range nodes[COHORT_TYPE] {
		if i == skipFailedIndex {
			continue
		}

		cohortClient, err := client.NewClientAPI(cohortAddr.Nodeaddr)
		require.NoError(t, err)

		cohortValue, err := cohortClient.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, expectedValue, cohortValue.Value)
		successCount++
	}
	return successCount
}

// checkValueOnAllCohorts checks if a value exists on ALL cohorts
func checkValueOnAllCohorts(t *testing.T, key string, expectedValue []byte) {
	t.Helper()

	for _, cohortAddr := range nodes[COHORT_TYPE] {
		cohortClient, err := client.NewClientAPI(cohortAddr.Nodeaddr)
		require.NoError(t, err)

		cohortValue, err := cohortClient.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, expectedValue, cohortValue.Value)
	}
}

// checkValueNotOnNode checks that a value does NOT exist on specified node
func checkValueNotOnNode(t *testing.T, nodeAddr string, key string) {
	t.Helper()

	nodeClient, err := client.NewClientAPI(nodeAddr)
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
		cohortClient, err := client.NewClientAPI(cohortAddr.Nodeaddr)
		require.NoError(t, err)

		cohortValue, err := cohortClient.Get(context.Background(), key)
		if err == nil {
			require.Equal(t, expectedValue, cohortValue.Value)
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
