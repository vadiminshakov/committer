package committer_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vadiminshakov/committer/v2"
)

// ledger is an in-memory Resource that rejects keys listed in reject.
type ledger struct {
	mu       sync.Mutex
	reject   map[string]string
	prepared map[uint64]committer.Tx
	applied  map[string][]byte
	aborted  []uint64
}

func newLedger() *ledger {
	return &ledger{
		reject:   map[string]string{},
		prepared: map[uint64]committer.Tx{},
		applied:  map[string][]byte{},
	}
}

func (l *ledger) Prepare(_ context.Context, tx committer.Tx) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if reason, ok := l.reject[tx.Key]; ok {
		return errors.New(reason)
	}

	l.prepared[tx.Height] = tx

	return nil
}

func (l *ledger) Commit(_ context.Context, tx committer.Tx) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.prepared, tx.Height)
	l.applied[tx.Key] = tx.Value

	return nil
}

func (l *ledger) Abort(_ context.Context, height uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.prepared[height]; ok {
		delete(l.prepared, height)
		l.aborted = append(l.aborted, height)
	}

	return nil
}

func (l *ledger) value(key string) ([]byte, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	value, ok := l.applied[key]

	return value, ok
}

func (l *ledger) abortedHeights() []uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]uint64(nil), l.aborted...)
}

func freeAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := listener.Addr().String()
	require.NoError(t, listener.Close())

	return addr
}

func TestCommitAcrossParticipants(t *testing.T) {
	for _, protocol := range []committer.Protocol{committer.TwoPhase, committer.ThreePhase} {
		t.Run(map[committer.Protocol]string{committer.TwoPhase: "2pc", committer.ThreePhase: "3pc"}[protocol], func(t *testing.T) {
			testCommitAcrossParticipants(t, protocol)
		})
	}
}

func testCommitAcrossParticipants(t *testing.T, protocol committer.Protocol) {
	ctx := context.Background()
	dataDir := t.TempDir()
	coordinatorAddr := freeAddr(t)
	ledgers := []*ledger{newLedger(), newLedger()}
	participantAddrs := make([]string, 0, len(ledgers))

	for _, resource := range ledgers {
		addr := freeAddr(t)
		participant, err := committer.StartParticipant(ctx, committer.ParticipantConfig{
			Addr:        addr,
			Coordinator: coordinatorAddr,
			Protocol:    protocol,
			Timeout:     time.Second,
			DataDir:     dataDir,
		}, resource)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, participant.Close()) })

		participantAddrs = append(participantAddrs, addr)
	}

	coordinator, err := committer.StartCoordinator(committer.CoordinatorConfig{
		Addr:         coordinatorAddr,
		Participants: participantAddrs,
		Protocol:     protocol,
		DataDir:      dataDir,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	height, err := coordinator.Commit(ctx, "order-1", []byte("paid"))
	require.NoError(t, err)
	require.Equal(t, committer.OutcomeCommitted, coordinator.Outcome(height))

	for _, resource := range ledgers {
		require.Eventually(t, func() bool {
			value, ok := resource.value("order-1")

			return ok && string(value) == "paid"
		}, 3*time.Second, 10*time.Millisecond)
	}

	// one participant votes NO: nobody applies the change, the other aborts
	ledgers[1].reject["order-2"] = "insufficient funds"

	height, err = coordinator.Commit(ctx, "order-2", []byte("paid"))
	require.ErrorIs(t, err, committer.ErrAborted)
	require.ErrorContains(t, err, "insufficient funds")
	require.Equal(t, committer.OutcomeAborted, coordinator.Outcome(height))

	require.Eventually(t, func() bool {
		return len(ledgers[0].abortedHeights()) == 1 && ledgers[0].abortedHeights()[0] == height
	}, 3*time.Second, 10*time.Millisecond)

	for _, resource := range ledgers {
		_, ok := resource.value("order-2")
		require.False(t, ok)
	}

	// the cluster keeps working after an abort
	delete(ledgers[1].reject, "order-2")

	_, err = coordinator.Commit(ctx, "order-2", []byte("paid"))
	require.NoError(t, err)

	_, err = coordinator.Commit(ctx, "", nil)
	require.ErrorIs(t, err, committer.ErrInvalidTransaction)
}

func TestClientReportsAbort(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	coordinatorAddr := freeAddr(t)
	participantAddr := freeAddr(t)

	resource := newLedger()
	resource.reject["k"] = "no"

	participant, err := committer.StartParticipant(ctx, committer.ParticipantConfig{
		Addr: participantAddr, Coordinator: coordinatorAddr, DataDir: dataDir,
	}, resource)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, participant.Close()) })

	coordinator, err := committer.StartCoordinator(committer.CoordinatorConfig{
		Addr: coordinatorAddr, Participants: []string{participantAddr}, DataDir: dataDir,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	client, err := committer.Dial(coordinatorAddr)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	_, err = client.Commit(ctx, "k", []byte("v"))
	require.ErrorIs(t, err, committer.ErrAborted)

	height, err := client.Commit(ctx, "other", []byte("v"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), height)
}

func TestStartValidatesConfig(t *testing.T) {
	_, err := committer.StartParticipant(context.Background(), committer.ParticipantConfig{
		Addr: "localhost:1", Coordinator: "localhost:2",
	}, nil)
	require.Error(t, err)

	_, err = committer.StartCoordinator(committer.CoordinatorConfig{Addr: "localhost:1"})
	require.Error(t, err)

	_, err = committer.StartCoordinator(committer.CoordinatorConfig{
		Addr: "localhost:1", Participants: []string{"nope"},
	})
	require.Error(t, err)
}
