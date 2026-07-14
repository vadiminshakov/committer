package commitalgo

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/vadiminshakov/committer/core/cohort/commitalgo/hooks"
	"github.com/vadiminshakov/committer/core/dto"

	"github.com/vadiminshakov/committer/io/store"
	iowal "github.com/vadiminshakov/committer/io/wal"
	"github.com/vadiminshakov/committer/mocks"
	"github.com/vadiminshakov/gowal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// findWalRecord scans the WAL and returns the first record with the given key.
func findWalRecord(wal *gowal.Wal, key string) (gowal.Record, bool) {
	for rec := range wal.Iterator() {
		if rec.Key == key {
			return rec, true
		}
	}

	return gowal.Record{}, false
}

// testHook for testing purposes
type testHook struct {
	proposeResult bool
	commitResult  bool
	proposeCalled bool
	commitCalled  bool
}

func (t *testHook) OnPropose(req *dto.ProposeRequest) bool {
	t.proposeCalled = true
	return t.proposeResult
}

func (t *testHook) OnCommit(req *dto.CommitRequest) bool {
	t.commitCalled = true
	return t.commitResult
}

func openTestWAL(t *testing.T, walPath string) *gowal.Wal {
	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	t.Cleanup(func() { wal.Close() })

	return wal
}

func newStateStore(t *testing.T, w *gowal.Wal) (*store.Store, *iowal.RecoveryState) {
	dbPath := filepath.Join(t.TempDir(), "badger")
	stateStore, recovery, err := store.New(iowal.New(w), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { stateStore.Close() })
	return stateStore, recovery
}

func prepareCommitter(t *testing.T, walPath, commitType string, timeout uint64, hooks ...hooks.Hook) (*CommitterImpl, *store.Store, *gowal.Wal, *iowal.RecoveryState) {
	w := openTestWAL(t, walPath)
	stateStore, recovery := newStateStore(t, w)

	committer := NewCommitter(stateStore, commitType, iowal.New(w), timeout, hooks...)
	committer.SetHeight(recovery.NextHeight)

	return committer, stateStore, w, recovery
}

func TestNewCommitter_DefaultHook(t *testing.T) {
	tempDir := t.TempDir()
	committer, _, _, _ := prepareCommitter(t, filepath.Join(tempDir, "wal"), "3pc", 5000)

	require.Equal(t, 1, committer.hookRegistry.Count(), "Expected 1 default hook")
}

func TestNewCommitter_CustomHooks(t *testing.T) {
	tempDir := t.TempDir()
	// test: create committer with custom hooks
	testHook1 := &testHook{proposeResult: true, commitResult: true}
	testHook2 := &testHook{proposeResult: true, commitResult: true}

	committer, _, _, _ := prepareCommitter(t, filepath.Join(tempDir, "wal"), "3pc", 5000, testHook1, testHook2)

	require.Equal(t, 2, committer.hookRegistry.Count(), "Expected 2 custom hooks")
}

func TestNewCommitter_DynamicHookRegistration(t *testing.T) {
	tempDir := t.TempDir()
	// test: create committer and add hooks dynamically
	committer, _, _, _ := prepareCommitter(t, filepath.Join(tempDir, "wal"), "3pc", 5000)

	require.Equal(t, 1, committer.hookRegistry.Count(), "Expected 1 default hook initially")

	// add hooks dynamically
	testHook := &testHook{proposeResult: true, commitResult: true}
	committer.RegisterHook(testHook)

	require.Equal(t, 2, committer.hookRegistry.Count(), "Expected 2 hooks after registration")
}

func TestNewCommitter_BuiltinHooks(t *testing.T) {
	tempDir := t.TempDir()

	metricsHook := hooks.NewMetricsHook()
	validationHook := hooks.NewValidationHook(100, 1024)
	auditHook := hooks.NewAuditHook("test_audit.log")

	committer, _, _, _ := prepareCommitter(t, filepath.Join(tempDir, "wal"), "3pc", 5000,
		metricsHook,
		validationHook,
		auditHook,
	)

	require.Equal(t, 3, committer.hookRegistry.Count(), "Expected 3 built-in hooks")

	proposeCount, commitCount, _ := metricsHook.GetStats()
	require.Equal(t, uint64(0), proposeCount, "Expected initial propose count to be 0")
	require.Equal(t, uint64(0), commitCount, "Expected initial commit count to be 0")
}

func TestCommit_ValidatesExternalRequest(t *testing.T) {
	tempDir := t.TempDir()
	committer, _, _, _ := prepareCommitter(t, filepath.Join(tempDir, "wal"), "two-phase", 5000)

	t.Run("nil request", func(t *testing.T) {
		resp, err := committer.Commit(context.Background(), nil)

		require.Nil(t, resp)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.Equal(t, uint64(0), committer.Height())
	})

	t.Run("cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		resp, err := committer.Commit(ctx, &dto.CommitRequest{Height: 0})

		require.Nil(t, resp)
		require.Equal(t, codes.Canceled, status.Code(err))
		require.Equal(t, uint64(0), committer.Height())
	})
}

func TestCommit_StateValidation_2PC(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "wal")
	wal := openTestWAL(t, walPath)

	stateStore, recovery := newStateStore(t, wal)

	// create 2PC committer
	committer := NewCommitter(stateStore, "two-phase", iowal.New(wal), 5000)
	committer.SetHeight(recovery.NextHeight)

	require.Equal(t, "propose", committer.getCurrentState())

	// first, propose a transaction
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}

	_, err := committer.Propose(context.Background(), proposeReq)
	require.NoError(t, err)

	// a 2PC cohort that voted YES enters the prepared state
	require.Equal(t, "prepared", committer.getCurrentState())

	// commit should work from prepared state
	commitReq := &dto.CommitRequest{Height: 0}
	resp, err := committer.Commit(context.Background(), commitReq)
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)

	// should return to propose state after commit
	require.Equal(t, "propose", committer.getCurrentState())
}

func TestCommit_StateValidation_3PC(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "wal")

	wal := openTestWAL(t, walPath)
	stateStore, recovery := newStateStore(t, wal)

	// create 3PC committer
	committer := NewCommitter(stateStore, "three-phase", iowal.New(wal), 5000)
	committer.SetHeight(recovery.NextHeight)

	require.Equal(t, "propose", committer.getCurrentState())

	// first, propose a transaction
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}

	_, err := committer.Propose(context.Background(), proposeReq)
	require.NoError(t, err)

	require.Equal(t, "prepared", committer.getCurrentState())

	// normal commit must fail before PRECOMMIT in 3PC mode
	commitReq := &dto.CommitRequest{Height: 0}
	_, err = committer.Commit(context.Background(), commitReq)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid state for commit: expected precommit for three-phase mode, but current state is prepared")

	// state should remain waiting after failed commit
	require.Equal(t, "prepared", committer.getCurrentState())

	// now go through proper 3PC flow: propose -> precommit -> commit
	_, err = committer.Precommit(context.Background(), 0)
	require.NoError(t, err)
	require.Equal(t, "precommit", committer.getCurrentState())

	// now commit should work from precommit state
	resp, err := committer.Commit(context.Background(), commitReq)
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)

	// should return to propose state after successful commit
	require.Equal(t, "propose", committer.getCurrentState())
}

func TestCommit_StateRestoration_OnErrors(t *testing.T) {
	t.Run("Hook failure", func(t *testing.T) {
		tempDir := t.TempDir()
		walPath := filepath.Join(tempDir, "wal")
		wal := openTestWAL(t, walPath)

		stateStore, recovery := newStateStore(t, wal)

		// create 3PC committer with a hook that will fail commit
		failingHook := &testHook{proposeResult: true, commitResult: false}
		committer := NewCommitter(stateStore, "three-phase", iowal.New(wal), 5000, failingHook)
		committer.SetHeight(recovery.NextHeight)

		// go through proper 3PC flow: propose -> precommit
		proposeReq := &dto.ProposeRequest{
			Height: 0,
			Key:    "test-key",
			Value:  []byte("test-value"),
		}

		_, err := committer.Propose(context.Background(), proposeReq)
		require.NoError(t, err)

		_, err = committer.Precommit(context.Background(), 0)
		require.NoError(t, err)
		require.Equal(t, "precommit", committer.getCurrentState())

		// commit should fail due to hook, but state should be restored
		commitReq := &dto.CommitRequest{Height: 0}
		resp, err := committer.Commit(context.Background(), commitReq)
		require.NoError(t, err)
		require.Equal(t, dto.ResponseTypeNack, resp.ResponseType)

		// a failed commit must keep the height fenced in PRECOMMIT
		require.Equal(t, "precommit", committer.getCurrentState())
		// height should not be incremented on hook failure
		require.Equal(t, uint64(0), committer.Height())
	})

	t.Run("Redelivered commit for already-aborted height is rejected", func(t *testing.T) {
		tempDir := t.TempDir()
		walPath := filepath.Join(tempDir, "wal")
		wal := openTestWAL(t, walPath)

		stateStore, recovery := newStateStore(t, wal)

		// create 3PC committer
		committer := NewCommitter(stateStore, "three-phase", iowal.New(wal), 5000)
		committer.SetHeight(recovery.NextHeight)

		// first propose a normal transaction
		proposeReq := &dto.ProposeRequest{
			Height: 0,
			Key:    "test-key",
			Value:  []byte("test-value"),
		}

		_, err := committer.Propose(context.Background(), proposeReq)
		require.NoError(t, err)

		require.Equal(t, "prepared", committer.getCurrentState())

		// abort before PRECOMMIT resolves the height: the abort is journaled and
		// the height is consumed
		abortResp, aerr := committer.Abort(context.Background(), &dto.AbortRequest{Height: 0, Reason: "test"})
		require.NoError(t, aerr)
		require.Equal(t, dto.ResponseTypeAck, abortResp.ResponseType)
		require.Equal(t, uint64(1), committer.Height())

		// a re-delivered commit for the aborted height must repeat the abort answer (NACK)
		commitReq := &dto.CommitRequest{Height: 0}
		resp, err := committer.Commit(context.Background(), commitReq)
		require.NoError(t, err)
		require.Equal(t, dto.ResponseTypeNack, resp.ResponseType)

		// state stays in propose, ready for the next height
		require.Equal(t, "propose", committer.getCurrentState())
		require.Equal(t, uint64(1), committer.Height())

		// data should not be in database (commit was skipped)
		value, err := stateStore.Get("test-key")
		require.Error(t, err) // should not exist
		require.Nil(t, value)
	})
}

func TestGetExpectedCommitState(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "wal")
	wal := openTestWAL(t, walPath)

	stateStore, _ := newStateStore(t, wal)

	// test 2PC mode
	committer2PC := NewCommitter(stateStore, "two-phase", iowal.New(wal), 5000)
	require.Equal(t, "prepared", committer2PC.getExpectedCommitState())

	// test 3PC mode
	committer3PC := NewCommitter(stateStore, "three-phase", iowal.New(wal), 5000)
	require.Equal(t, "precommit", committer3PC.getExpectedCommitState())
}
func TestPrecommitTimeout_StateValidation(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "wal")
	wal := openTestWAL(t, walPath)

	stateStore, recovery := newStateStore(t, wal)

	// сreate 3PC committer with short timeout for testing
	committer := NewCommitter(stateStore, "three-phase", iowal.New(wal), 50) // 50ms timeout
	committer.SetHeight(recovery.NextHeight)

	// test case 1: should skip autocommit when in commit state
	// first go to prepared, then precommit, then commit
	committer.state.Transition(preparedStage)
	committer.state.Transition(precommitStage)
	committer.state.Transition(commitStage)
	committer.handlePrecommitTimeout(0)
	require.Equal(t, "commit", committer.getCurrentState()) // state unchanged

	// test case 2: should skip autocommit when in propose state
	// transition back to propose (commit -> propose is valid)
	committer.state.Transition(proposeStage)
	committer.handlePrecommitTimeout(0)
	require.Equal(t, "propose", committer.getCurrentState()) // state unchanged

	// test case 3: should skip autocommit when height doesn't match
	committer.state.Transition(preparedStage)
	committer.state.Transition(precommitStage)
	committer.handlePrecommitTimeout(999)                      // wrong height
	require.Equal(t, "precommit", committer.getCurrentState()) // state unchanged
}

func TestPrecommitTimeout_AutocommitSuccess(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "wal")
	wal := openTestWAL(t, walPath)

	stateStore, recovery := newStateStore(t, wal)

	// create 3PC committer
	committer := NewCommitter(stateStore, "three-phase", iowal.New(wal), 50)
	committer.SetHeight(recovery.NextHeight)

	ctx := context.Background()

	// first propose to get data in WAL
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}
	_, err := committer.Propose(ctx, proposeReq)
	require.NoError(t, err)

	// move to precommit state
	_, err = committer.Precommit(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, "precommit", committer.getCurrentState())

	// test successful autocommit
	committer.handlePrecommitTimeout(0)

	// should be back in propose state after successful autocommit
	require.Equal(t, "propose", committer.getCurrentState())
	require.Equal(t, uint64(1), committer.Height()) // height should be incremented
}

func TestPrecommitTimeout_AutocommitWithSkipRecord(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "wal")
	wal := openTestWAL(t, walPath)

	stateStore, recovery := newStateStore(t, wal)

	// create 3PC committer
	committer := NewCommitter(stateStore, "three-phase", iowal.New(wal), 50)
	committer.SetHeight(recovery.NextHeight)

	// abort fully resolves height 0: journals the abort and consumes the height
	abortResp, err := committer.Abort(context.Background(), &dto.AbortRequest{Height: 0, Reason: "test"})
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeAck, abortResp.ResponseType)
	require.Equal(t, uint64(1), committer.Height())

	// a late precommit timeout for the aborted height is a no-op
	committer.state.Transition(preparedStage)
	committer.state.Transition(precommitStage)
	committer.handlePrecommitTimeout(0)

	// height mismatch prevents autocommit of the aborted transaction
	require.Equal(t, "precommit", committer.getCurrentState())
	require.Equal(t, uint64(1), committer.Height())
}

func TestPrecommitTimeout_AutocommitFailure(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "wal")
	wal := openTestWAL(t, walPath)

	stateStore, recovery := newStateStore(t, wal)

	// create 3PC committer with failing hook
	failingHook := &testHook{proposeResult: true, commitResult: false}
	committer := NewCommitter(stateStore, "three-phase", iowal.New(wal), 50, failingHook)
	committer.SetHeight(recovery.NextHeight)

	ctx := context.Background()

	// first propose to get data in WAL
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}
	_, err := committer.Propose(ctx, proposeReq)
	require.NoError(t, err)

	// move to precommit state
	_, err = committer.Precommit(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, "precommit", committer.getCurrentState())

	// test autocommit failure
	committer.handlePrecommitTimeout(0)

	require.Equal(t, "precommit", committer.getCurrentState())
	require.Equal(t, uint64(0), committer.Height()) // height should not be incremented on failure
}

func TestPrecommitTimeout_NoDataInWAL(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "wal")
	wal := openTestWAL(t, walPath)

	stateStore, recovery := newStateStore(t, wal)

	// create 3PC committer
	committer := NewCommitter(stateStore, "three-phase", iowal.New(wal), 50)
	committer.SetHeight(recovery.NextHeight)

	// set up state without data in WAL

	// move to precommit state without proposing first
	committer.state.Transition(preparedStage)
	committer.state.Transition(precommitStage)
	require.Equal(t, "precommit", committer.getCurrentState())

	// test autocommit with no data in WAL - it remains fenced
	committer.handlePrecommitTimeout(0)

	// no abort or state rollback is allowed
	require.Equal(t, "precommit", committer.getCurrentState())
	require.Equal(t, uint64(0), committer.Height()) // height should not be incremented
}

func TestRecoverToPropose(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "wal")
	wal := openTestWAL(t, walPath)

	stateStore, recovery := newStateStore(t, wal)

	// create 3PC committer
	committer := NewCommitter(stateStore, "three-phase", iowal.New(wal), 50)
	committer.SetHeight(recovery.NextHeight)

	// test recovery from precommit state
	committer.state.Transition(preparedStage)
	committer.state.Transition(precommitStage)
	require.Equal(t, "precommit", committer.getCurrentState())

	committer.resetToPropose(0, "test")
	require.Equal(t, "propose", committer.getCurrentState())

	// test recovery from commit state (should transition to propose)
	// first to precommit, then to commit
	committer.state.Transition(preparedStage)
	committer.state.Transition(precommitStage)
	committer.state.Transition(commitStage)
	require.Equal(t, "commit", committer.getCurrentState())

	committer.resetToPropose(0, "test")
	// should transition to propose since commit -> propose is valid in FSM
	require.Equal(t, "propose", committer.getCurrentState())
}

func TestAbort_CurrentHeight(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "wal")
	wal := openTestWAL(t, walPath)

	stateStore, recovery := newStateStore(t, wal)

	// create 3PC committer
	committer := NewCommitter(stateStore, "three-phase", iowal.New(wal), 5000)
	committer.SetHeight(recovery.NextHeight)

	// set up a transaction at current height
	ctx := context.Background()
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}

	_, err := committer.Propose(ctx, proposeReq)
	require.NoError(t, err)

	// move to precommit state
	_, err = committer.Precommit(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, "precommit", committer.getCurrentState())

	// ABORT is rejected after PRECOMMIT
	abortReq := &dto.AbortRequest{
		Height: 0,
		Reason: "Test abort",
	}

	resp, err := committer.Abort(ctx, abortReq)
	require.Error(t, err)
	require.Equal(t, dto.ResponseTypeNack, resp.ResponseType)

	require.Equal(t, "precommit", committer.getCurrentState())

	// should have the original data in WAL (Prepared)
	prepRec, ok := findWalRecord(wal, iowal.PreparedKey(0))
	require.True(t, ok, "PREPARED record should be in WAL")

	// verify payload
	walTx, err := iowal.Decode(prepRec.Value)
	require.NoError(t, err)
	require.Equal(t, "test-key", walTx.Key)

	// verify no Abort was written after PRECOMMIT
	_, ok = findWalRecord(wal, iowal.AbortKey(0))
	require.False(t, ok, "ABORT record must not be in WAL")

	value, err := stateStore.Get("test-key")
	require.Error(t, err, "Original value should not exist in database after abort")
	require.Nil(t, value)
}

func TestAbort_FutureHeight(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "wal")
	wal := openTestWAL(t, walPath)

	stateStore, recovery := newStateStore(t, wal)

	// create committer
	committer := NewCommitter(stateStore, "three-phase", iowal.New(wal), 5000)
	committer.SetHeight(recovery.NextHeight)

	// test abort for future height (should be ignored)
	ctx := context.Background()
	abortReq := &dto.AbortRequest{
		Height: 10, // future height
		Reason: "Test abort future",
	}

	resp, err := committer.Abort(ctx, abortReq)
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)

	// state should remain unchanged
	require.Equal(t, "propose", committer.getCurrentState())
	require.Equal(t, uint64(0), committer.Height())

	require.Equal(t, uint64(0), wal.CurrentIndex(), "WAL should be empty when abort was for future height")
}

func TestAbort_PastHeight(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "wal")
	wal := openTestWAL(t, walPath)

	stateStore, recovery := newStateStore(t, wal)

	// create committer and advance height
	committer := NewCommitter(stateStore, "two-phase", iowal.New(wal), 5000)
	committer.SetHeight(recovery.NextHeight)

	// complete a transaction to advance height
	ctx := context.Background()
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}

	_, err := committer.Propose(ctx, proposeReq)
	require.NoError(t, err)

	commitReq := &dto.CommitRequest{Height: 0}
	_, err = committer.Commit(ctx, commitReq)
	require.NoError(t, err)

	// height should now be 1
	require.Equal(t, uint64(1), committer.Height())

	// test abort for past height (should be ignored)
	abortReq := &dto.AbortRequest{
		Height: 0, // Past height
		Reason: "Test abort past",
	}

	resp, err := committer.Abort(ctx, abortReq)
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)

	// state should remain unchanged
	require.Equal(t, "propose", committer.getCurrentState())
	require.Equal(t, uint64(1), committer.Height())

	// check wal unchanged
	_, ok := findWalRecord(wal, iowal.PreparedKey(0))
	require.True(t, ok, "PREPARED record should be in WAL")
	_, ok = findWalRecord(wal, iowal.CommitKey(0))
	require.True(t, ok, "COMMIT record should be in WAL")

	// check normal data is in db
	value, err := stateStore.Get("test-key")
	require.NoError(t, err, "Data should exist in database for past committed transaction")
	require.Equal(t, "test-value", string(value))
}

func TestAbort_StateRecovery_3PC(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "wal")
	wal := openTestWAL(t, walPath)

	stateStore, recovery := newStateStore(t, wal)

	// create 3PC committer
	committer := NewCommitter(stateStore, "three-phase", iowal.New(wal), 5000)
	committer.SetHeight(recovery.NextHeight)

	// set up transaction and move to precommit state
	ctx := context.Background()
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}

	_, err := committer.Propose(ctx, proposeReq)
	require.NoError(t, err)

	_, err = committer.Precommit(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, "precommit", committer.getCurrentState())

	// test abort from precommit state
	abortReq := &dto.AbortRequest{
		Height: 0,
		Reason: "Test 3PC abort",
	}

	resp, err := committer.Abort(ctx, abortReq)
	require.Error(t, err)
	require.Equal(t, dto.ResponseTypeNack, resp.ResponseType)

	require.Equal(t, "precommit", committer.getCurrentState())

	// check wal: no abort may follow precommit
	_, ok := findWalRecord(wal, iowal.AbortKey(0))
	require.False(t, ok, "ABORT record must not be in WAL")
}

func TestAbort_StateRecovery_2PC(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "wal")
	wal := openTestWAL(t, walPath)

	stateStore, recovery := newStateStore(t, wal)

	// create 2PC committer
	committer := NewCommitter(stateStore, "two-phase", iowal.New(wal), 5000)
	committer.SetHeight(recovery.NextHeight)

	// set up transaction (in 2PC, we stay in propose state)
	ctx := context.Background()
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}

	_, err := committer.Propose(ctx, proposeReq)
	require.NoError(t, err)
	require.Equal(t, "prepared", committer.getCurrentState())

	// test abort from prepared state in 2PC
	abortReq := &dto.AbortRequest{
		Height: 0,
		Reason: "Test 2PC abort",
	}

	resp, err := committer.Abort(ctx, abortReq)
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)

	// abort resolves the height and returns the cohort to propose
	require.Equal(t, "propose", committer.getCurrentState())
	require.Equal(t, uint64(1), committer.Height())

	// check wal
	_, ok := findWalRecord(wal, iowal.AbortKey(0))
	require.True(t, ok, "ABORT record should be in WAL")
}

func TestPropose_HeightMismatch(t *testing.T) {
	tempDir := t.TempDir()
	committer, _, _, _ := prepareCommitter(t, filepath.Join(tempDir, "wal"), "two-phase", 5000)

	// a proposal for a non-current height must be NACKed with our height
	resp, err := committer.Propose(context.Background(), &dto.ProposeRequest{
		Height: 5,
		Key:    "test-key",
		Value:  []byte("test-value"),
	})
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeNack, resp.ResponseType)
	require.Equal(t, uint64(0), resp.Height)
	require.Equal(t, "propose", committer.getCurrentState())
}

func TestPropose_RedeliveryWhilePrepared(t *testing.T) {
	tempDir := t.TempDir()
	committer, stateStore, _, _ := prepareCommitter(t, filepath.Join(tempDir, "wal"), "two-phase", 5000)

	ctx := context.Background()
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}

	_, err := committer.Propose(ctx, proposeReq)
	require.NoError(t, err)
	require.Equal(t, "prepared", committer.getCurrentState())

	// re-delivered identical proposal: repeat the YES vote
	resp, err := committer.Propose(ctx, proposeReq)
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)
	require.Equal(t, "prepared", committer.getCurrentState())

	// conflicting proposal for the same height: the vote must not change
	resp, err = committer.Propose(ctx, &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("other-value"),
	})
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeNack, resp.ResponseType)

	// original vote is intact: commit applies the original payload
	_, err = committer.Commit(ctx, &dto.CommitRequest{Height: 0})
	require.NoError(t, err)

	value, err := stateStore.Get("test-key")
	require.NoError(t, err)
	require.Equal(t, "test-value", string(value))
}

func TestAbort_NextTransactionSucceeds(t *testing.T) {
	tempDir := t.TempDir()
	committer, stateStore, _, _ := prepareCommitter(t, filepath.Join(tempDir, "wal"), "two-phase", 5000)

	ctx := context.Background()

	// transaction at height 0 gets aborted
	_, err := committer.Propose(ctx, &dto.ProposeRequest{Height: 0, Key: "k1", Value: []byte("v1")})
	require.NoError(t, err)

	_, err = committer.Abort(ctx, &dto.AbortRequest{Height: 0, Reason: "test"})
	require.NoError(t, err)
	require.Equal(t, uint64(1), committer.Height())

	// the next transaction (at the next height) must commit normally:
	// the abort of height 0 must not poison it
	_, err = committer.Propose(ctx, &dto.ProposeRequest{Height: 1, Key: "k2", Value: []byte("v2")})
	require.NoError(t, err)

	resp, err := committer.Commit(ctx, &dto.CommitRequest{Height: 1})
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)
	require.Equal(t, uint64(2), committer.Height())

	value, err := stateStore.Get("k2")
	require.NoError(t, err)
	require.Equal(t, "v2", string(value))

	_, err = stateStore.Get("k1")
	require.Error(t, err, "aborted transaction data must not be applied")
}

func TestCommit_RedeliveredDecision(t *testing.T) {
	tempDir := t.TempDir()
	committer, _, _, _ := prepareCommitter(t, filepath.Join(tempDir, "wal"), "two-phase", 5000)

	ctx := context.Background()

	// height 0: committed
	_, err := committer.Propose(ctx, &dto.ProposeRequest{Height: 0, Key: "k1", Value: []byte("v1")})
	require.NoError(t, err)
	_, err = committer.Commit(ctx, &dto.CommitRequest{Height: 0})
	require.NoError(t, err)

	// height 1: aborted
	_, err = committer.Propose(ctx, &dto.ProposeRequest{Height: 1, Key: "k2", Value: []byte("v2")})
	require.NoError(t, err)
	_, err = committer.Abort(ctx, &dto.AbortRequest{Height: 1, Reason: "test"})
	require.NoError(t, err)

	// re-delivered commit repeats the recorded answer, not a blanket ACK
	resp, err := committer.Commit(ctx, &dto.CommitRequest{Height: 0})
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType, "committed height must re-ACK")

	resp, err = committer.Commit(ctx, &dto.CommitRequest{Height: 1})
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeNack, resp.ResponseType, "aborted height must NACK a commit")
}

func TestResume_InDoubtTransaction2PC(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "wal")
	wal := openTestWAL(t, walPath)
	stateStore, _ := newStateStore(t, wal)

	payload, err := iowal.Encode(iowal.Tx{Key: "test-key", Value: []byte("test-value")})
	require.NoError(t, err)

	// simulate a restart of a cohort that crashed while prepared at height 3
	committer := NewCommitter(stateStore, "two-phase", iowal.New(wal), 5000)
	committer.Resume(&iowal.RecoveryState{
		NextHeight: 4,
		Unresolved: &iowal.UnresolvedTransaction{
			Height:  3,
			Phase:   iowal.PhaseKeyPrepared,
			Payload: payload,
		},
		Decisions: map[uint64]string{2: iowal.PhaseKeyCommit},
	})

	require.Equal(t, uint64(3), committer.Height())
	require.Equal(t, "prepared", committer.getCurrentState(), "cohort must re-enter prepared state")

	// the coordinator's re-delivered commit resolves the in-doubt transaction
	resp, err := committer.Commit(context.Background(), &dto.CommitRequest{Height: 3})
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)
	require.Equal(t, uint64(4), committer.Height())

	value, err := stateStore.Get("test-key")
	require.NoError(t, err)
	require.Equal(t, "test-value", string(value))
}

func TestResume_PrecommittedTransaction3PC(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "wal")
	wal := openTestWAL(t, walPath)
	stateStore, _ := newStateStore(t, wal)

	payload, err := iowal.Encode(iowal.Tx{Key: "test-key", Value: []byte("test-value")})
	require.NoError(t, err)

	committer := NewCommitter(stateStore, "three-phase", iowal.New(wal), 60_000)
	committer.Resume(&iowal.RecoveryState{
		NextHeight: 4,
		Unresolved: &iowal.UnresolvedTransaction{
			Height:  3,
			Phase:   iowal.PhaseKeyPrecommit,
			Payload: payload,
		},
	})

	require.Equal(t, uint64(3), committer.Height())
	require.Equal(t, precommitStage, committer.getCurrentState())
	require.Equal(t, payload, committer.pendingPayload)
}

func TestTerminationProtocol_CommitDecision(t *testing.T) {
	tempDir := t.TempDir()
	committer, stateStore, _, _ := prepareCommitter(t, filepath.Join(tempDir, "wal"), "two-phase", 50)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	decisionRequester := mocks.NewMockDecisionRequester(ctrl)
	decisionRequester.EXPECT().Decision(gomock.Any(), uint64(0)).Return(dto.OutcomeCommit, nil).AnyTimes()
	committer.SetDecisionRequester(decisionRequester)

	_, err := committer.Propose(context.Background(), &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	})
	require.NoError(t, err)
	require.Equal(t, "prepared", committer.getCurrentState())

	// no commit arrives from the coordinator; the cohort must resolve the
	// in-doubt transaction itself by asking for the decision
	require.Eventually(t, func() bool {
		return committer.Height() == 1
	}, 3*time.Second, 20*time.Millisecond, "cohort must commit via termination protocol")

	require.Equal(t, "propose", committer.getCurrentState())
	value, err := stateStore.Get("test-key")
	require.NoError(t, err)
	require.Equal(t, "test-value", string(value))
}

func TestTerminationProtocol_AbortDecision(t *testing.T) {
	tempDir := t.TempDir()
	committer, stateStore, wal, _ := prepareCommitter(t, filepath.Join(tempDir, "wal"), "two-phase", 50)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	decisionRequester := mocks.NewMockDecisionRequester(ctrl)
	decisionRequester.EXPECT().Decision(gomock.Any(), uint64(0)).Return(dto.OutcomeAbort, nil).AnyTimes()
	committer.SetDecisionRequester(decisionRequester)

	_, err := committer.Propose(context.Background(), &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return committer.Height() == 1
	}, 3*time.Second, 20*time.Millisecond, "cohort must abort via termination protocol")

	require.Equal(t, "propose", committer.getCurrentState())

	_, err = stateStore.Get("test-key")
	require.Error(t, err, "aborted transaction data must not be applied")

	_, ok := findWalRecord(wal, iowal.AbortKey(0))
	require.True(t, ok, "ABORT record should be in WAL")
}

func TestPreparedThreePhaseRecoveryUsesNormalPrecommitAndCommit(t *testing.T) {
	tempDir := t.TempDir()
	committer, stateStore, wal, _ := prepareCommitter(t, filepath.Join(tempDir, "wal"), "three-phase", 5_000)
	payload, err := iowal.Encode(iowal.Tx{Key: "test-key", Value: []byte("test-value")})
	require.NoError(t, err)
	committer.Resume(&iowal.RecoveryState{
		NextHeight: 1,
		Unresolved: &iowal.UnresolvedTransaction{
			Height:  0,
			Phase:   iowal.PhaseKeyPrepared,
			Payload: payload,
		},
	})

	ctx := context.Background()
	require.Equal(t, preparedStage, committer.getCurrentState())
	_, err = committer.Commit(ctx, &dto.CommitRequest{Height: 0})
	require.Error(t, err, "recovered 3PC PREPARED must not commit directly")
	require.Equal(t, preparedStage, committer.getCurrentState())

	resp, err := committer.Precommit(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)
	require.Equal(t, precommitStage, committer.getCurrentState())

	resp, err = committer.Commit(ctx, &dto.CommitRequest{Height: 0})
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)
	require.Equal(t, uint64(1), committer.Height())

	value, err := stateStore.Get("test-key")
	require.NoError(t, err)
	require.Equal(t, "test-value", string(value))

	// The final decision is idempotent after the normal legal phase sequence.
	resp, err = committer.Commit(ctx, &dto.CommitRequest{Height: 0})
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)

	_, ok := findWalRecord(wal, iowal.CommitKey(0))
	require.True(t, ok, "commit must be journaled")
}

func TestCommitRetriesFromCommitStageAfterStoreFailure(t *testing.T) {
	tempDir := t.TempDir()
	w := openTestWAL(t, filepath.Join(tempDir, "wal"))

	ctrl := gomock.NewController(t)
	stateStore := mocks.NewMockCommitalgoStateStore(ctrl)
	applyErr := errors.New("store unavailable")
	stateStore.EXPECT().Put("test-key", []byte("test-value")).Return(applyErr)
	stateStore.EXPECT().Put("test-key", []byte("test-value")).Return(nil)

	committer := NewCommitter(stateStore, "three-phase", iowal.New(w), 5_000)
	ctx := context.Background()
	_, err := committer.Propose(ctx, &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	})
	require.NoError(t, err)
	_, err = committer.Precommit(ctx, 0)
	require.NoError(t, err)

	_, err = committer.Commit(ctx, &dto.CommitRequest{Height: 0})
	require.ErrorIs(t, err, applyErr)
	require.Equal(t, commitStage, committer.getCurrentState())
	require.Equal(t, uint64(0), committer.Height())
	require.NotNil(t, committer.pendingPayload)

	resp, err := committer.Commit(ctx, &dto.CommitRequest{Height: 0})
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)
	require.Equal(t, proposeStage, committer.getCurrentState())
	require.Equal(t, uint64(1), committer.Height())
}
