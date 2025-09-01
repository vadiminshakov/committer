package commitalgo

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vadiminshakov/committer/core/cohort/commitalgo/hooks"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/io/db"
	"github.com/vadiminshakov/gowal"
)

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

func TestNewCommitter_DefaultHook(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "db")
	walPath := filepath.Join(tempDir, "wal")

	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	defer wal.Close()

	// test: create committer without hooks (should use default)
	committer := NewCommitter(database, "3pc", wal, 5000)

	require.Equal(t, 1, committer.hookRegistry.Count(), "Expected 1 default hook")
}

func TestNewCommitter_CustomHooks(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "db")
	walPath := filepath.Join(tempDir, "wal")

	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	defer wal.Close()

	// test: create committer with custom hooks
	testHook1 := &testHook{proposeResult: true, commitResult: true}
	testHook2 := &testHook{proposeResult: true, commitResult: true}

	committer := NewCommitter(database, "3pc", wal, 5000, testHook1, testHook2)

	require.Equal(t, 2, committer.hookRegistry.Count(), "Expected 2 custom hooks")
}

func TestNewCommitter_DynamicHookRegistration(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "db")
	walPath := filepath.Join(tempDir, "wal")

	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	defer wal.Close()

	// test: create committer and add hooks dynamically
	committer := NewCommitter(database, "3pc", wal, 5000)

	require.Equal(t, 1, committer.hookRegistry.Count(), "Expected 1 default hook initially")

	// add hooks dynamically
	testHook := &testHook{proposeResult: true, commitResult: true}
	committer.RegisterHook(testHook)

	require.Equal(t, 2, committer.hookRegistry.Count(), "Expected 2 hooks after registration")
}

func TestNewCommitter_BuiltinHooks(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "db")
	walPath := filepath.Join(tempDir, "wal")

	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	defer wal.Close()

	metricsHook := hooks.NewMetricsHook()
	validationHook := hooks.NewValidationHook(100, 1024)
	auditHook := hooks.NewAuditHook("test_audit.log")

	committer := NewCommitter(database, "3pc", wal, 5000,
		metricsHook,
		validationHook,
		auditHook,
	)

	require.Equal(t, 3, committer.hookRegistry.Count(), "Expected 3 built-in hooks")

	proposeCount, commitCount, _ := metricsHook.GetStats()
	require.Equal(t, uint64(0), proposeCount, "Expected initial propose count to be 0")
	require.Equal(t, uint64(0), commitCount, "Expected initial commit count to be 0")
}
func TestCommit_StateValidation_2PC(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "db")
	walPath := filepath.Join(tempDir, "wal")

	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	defer wal.Close()

	// create 2PC committer
	committer := NewCommitter(database, "two-phase", wal, 5000)

	require.Equal(t, "propose", committer.getCurrentState())

	// first, propose a transaction
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}

	_, err = committer.Propose(context.Background(), proposeReq)
	require.NoError(t, err)

	// should still be in propose state for 2PC
	require.Equal(t, "propose", committer.getCurrentState())

	// commit should work from propose state
	commitReq := &dto.CommitRequest{Height: 0}
	resp, err := committer.Commit(context.Background(), commitReq)
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)

	// should return to propose state after commit
	require.Equal(t, "propose", committer.getCurrentState())
}

func TestCommit_StateValidation_3PC(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "db")
	walPath := filepath.Join(tempDir, "wal")

	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	defer wal.Close()

	// create 3PC committer
	committer := NewCommitter(database, "three-phase", wal, 5000)

	require.Equal(t, "propose", committer.getCurrentState())

	// first, propose a transaction
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}

	_, err = committer.Propose(context.Background(), proposeReq)
	require.NoError(t, err)

	// should still be in propose state
	require.Equal(t, "propose", committer.getCurrentState())

	// commit should fail from propose state in 3PC mode
	commitReq := &dto.CommitRequest{Height: 0}
	_, err = committer.Commit(context.Background(), commitReq)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid state for commit: expected precommit for three-phase mode, but current state is propose")

	// state should remain in propose after failed commit
	require.Equal(t, "propose", committer.getCurrentState())

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
		dbPath := filepath.Join(tempDir, "db")
		walPath := filepath.Join(tempDir, "wal")

		database, err := db.New(dbPath)
		require.NoError(t, err)
		defer database.Close()

		walConfig := gowal.Config{
			Dir:              walPath,
			Prefix:           "test",
			SegmentThreshold: 1024,
			MaxSegments:      10,
			IsInSyncDiskMode: false,
		}

		wal, err := gowal.NewWAL(walConfig)
		require.NoError(t, err)
		defer wal.Close()

		// create 3PC committer with a hook that will fail commit
		failingHook := &testHook{proposeResult: true, commitResult: false}
		committer := NewCommitter(database, "three-phase", wal, 5000, failingHook)

		// go through proper 3PC flow: propose -> precommit
		proposeReq := &dto.ProposeRequest{
			Height: 0,
			Key:    "test-key",
			Value:  []byte("test-value"),
		}

		_, err = committer.Propose(context.Background(), proposeReq)
		require.NoError(t, err)

		_, err = committer.Precommit(context.Background(), 0)
		require.NoError(t, err)
		require.Equal(t, "precommit", committer.getCurrentState())

		// commit should fail due to hook, but state should be restored
		commitReq := &dto.CommitRequest{Height: 0}
		resp, err := committer.Commit(context.Background(), commitReq)
		require.NoError(t, err)
		require.Equal(t, dto.ResponseTypeNack, resp.ResponseType)

		// state should be restored to propose after failed commit
		require.Equal(t, "propose", committer.getCurrentState())
		// height should not be incremented on hook failure
		require.Equal(t, uint64(0), committer.Height())
	})

	t.Run("Skip record (tombstone)", func(t *testing.T) {
		tempDir := t.TempDir()
		dbPath := filepath.Join(tempDir, "db")
		walPath := filepath.Join(tempDir, "wal")

		database, err := db.New(dbPath)
		require.NoError(t, err)
		defer database.Close()

		walConfig := gowal.Config{
			Dir:              walPath,
			Prefix:           "test",
			SegmentThreshold: 1024,
			MaxSegments:      10,
			IsInSyncDiskMode: false,
		}

		wal, err := gowal.NewWAL(walConfig)
		require.NoError(t, err)
		defer wal.Close()

		// create 3PC committer
		committer := NewCommitter(database, "three-phase", wal, 5000)

		// first propose a normal transaction
		proposeReq := &dto.ProposeRequest{
			Height: 0,
			Key:    "test-key",
			Value:  []byte("test-value"),
		}

		_, err = committer.Propose(context.Background(), proposeReq)
		require.NoError(t, err)

		_, err = committer.Precommit(context.Background(), 0)
		require.NoError(t, err)
		require.Equal(t, "precommit", committer.getCurrentState())

		// simulate abort by writing skip record to WAL (this would normally be done by Abort method)
		err = wal.Write(0, "skip", nil)
		require.NoError(t, err)

		// commit should detect skip record and restore state
		commitReq := &dto.CommitRequest{Height: 0}
		resp, err := committer.Commit(context.Background(), commitReq)
		require.NoError(t, err)
		require.Equal(t, dto.ResponseTypeNack, resp.ResponseType)

		// state should be restored to propose after detecting skip record
		require.Equal(t, "propose", committer.getCurrentState())
		// height should not be incremented for skip records
		require.Equal(t, uint64(0), committer.Height())

		// data should not be in database (commit was skipped)
		value, err := database.Get("test-key")
		require.Error(t, err) // should not exist
		require.Nil(t, value)
	})
}

func TestGetExpectedCommitState(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "db")
	walPath := filepath.Join(tempDir, "wal")

	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	defer wal.Close()

	// test 2PC mode
	committer2PC := NewCommitter(database, "two-phase", wal, 5000)
	require.Equal(t, "propose", committer2PC.getExpectedCommitState())

	// test 3PC mode
	committer3PC := NewCommitter(database, "three-phase", wal, 5000)
	require.Equal(t, "precommit", committer3PC.getExpectedCommitState())
}
func TestPrecommitTimeout_StateValidation(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "db")
	walPath := filepath.Join(tempDir, "wal")

	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	defer wal.Close()

	// сreate 3PC committer with short timeout for testing
	committer := NewCommitter(database, "three-phase", wal, 50) // 50ms timeout

	ctx := context.Background()

	// test case 1: should skip autocommit when in commit state
	// first go to precommit, then to commit
	committer.state.Transition(precommitStage)
	committer.state.Transition(commitStage)
	committer.handlePrecommitTimeout(ctx, 0)
	require.Equal(t, "commit", committer.getCurrentState()) // state unchanged

	// test case 2: should skip autocommit when in propose state
	// transition back to propose (commit -> propose is valid)
	committer.state.Transition(proposeStage)
	committer.handlePrecommitTimeout(ctx, 0)
	require.Equal(t, "propose", committer.getCurrentState()) // state unchanged

	// test case 3: should skip autocommit when height doesn't match
	committer.state.Transition(precommitStage)
	committer.handlePrecommitTimeout(ctx, 999)                 // wrong height
	require.Equal(t, "precommit", committer.getCurrentState()) // state unchanged
}

func TestPrecommitTimeout_AutocommitSuccess(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "db")
	walPath := filepath.Join(tempDir, "wal")

	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	defer wal.Close()

	// create 3PC committer
	committer := NewCommitter(database, "three-phase", wal, 50)

	ctx := context.Background()

	// first propose to get data in WAL
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}
	_, err = committer.Propose(ctx, proposeReq)
	require.NoError(t, err)

	// move to precommit state
	_, err = committer.Precommit(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, "precommit", committer.getCurrentState())

	// test successful autocommit
	committer.handlePrecommitTimeout(ctx, 0)

	// should be back in propose state after successful autocommit
	require.Equal(t, "propose", committer.getCurrentState())
	require.Equal(t, uint64(1), committer.Height()) // height should be incremented
}

func TestPrecommitTimeout_AutocommitWithSkipRecord(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "db")
	walPath := filepath.Join(tempDir, "wal")

	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	defer wal.Close()

	// create 3PC committer
	committer := NewCommitter(database, "three-phase", wal, 50)

	ctx := context.Background()

	// write skip record directly to WAL
	err = wal.Write(0, "skip", nil)
	require.NoError(t, err)

	// move to precommit state
	committer.state.Transition(precommitStage)
	require.Equal(t, "precommit", committer.getCurrentState())

	// test autocommit with skip record - should recover to propose
	committer.handlePrecommitTimeout(ctx, 0)

	// should be back in propose state after recovery
	require.Equal(t, "propose", committer.getCurrentState())
	require.Equal(t, uint64(0), committer.Height()) // Hhight should not be incremented for skip
}

func TestPrecommitTimeout_AutocommitFailure(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "db")
	walPath := filepath.Join(tempDir, "wal")

	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	defer wal.Close()

	// create 3PC committer with failing hook
	failingHook := &testHook{proposeResult: true, commitResult: false}
	committer := NewCommitter(database, "three-phase", wal, 50, failingHook)

	ctx := context.Background()

	// first propose to get data in WAL
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}
	_, err = committer.Propose(ctx, proposeReq)
	require.NoError(t, err)

	// move to precommit state
	_, err = committer.Precommit(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, "precommit", committer.getCurrentState())

	// test autocommit failure - should recover to propose
	committer.handlePrecommitTimeout(ctx, 0)

	// should be back in propose state after recovery from failed autocommit
	require.Equal(t, "propose", committer.getCurrentState())
	require.Equal(t, uint64(0), committer.Height()) // height should not be incremented on failure
}

func TestPrecommitTimeout_NoDataInWAL(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "db")
	walPath := filepath.Join(tempDir, "wal")

	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	defer wal.Close()

	// create 3PC committer
	committer := NewCommitter(database, "three-phase", wal, 50)

	// set up state without data in WAL
	ctx := context.Background()

	// move to precommit state without proposing first
	committer.state.Transition(precommitStage)
	require.Equal(t, "precommit", committer.getCurrentState())

	// test autocommit with no data in WAL - should recover to propose
	committer.handlePrecommitTimeout(ctx, 0)

	// should be back in propose state after recovery
	require.Equal(t, "propose", committer.getCurrentState())
	require.Equal(t, uint64(0), committer.Height()) // height should not be incremented
}

func TestRecoverToPropose(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "db")
	walPath := filepath.Join(tempDir, "wal")

	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	defer wal.Close()

	// create 3PC committer
	committer := NewCommitter(database, "three-phase", wal, 50)

	// test recovery from precommit state
	committer.state.Transition(precommitStage)
	require.Equal(t, "precommit", committer.getCurrentState())

	committer.recoverToPropose(0)
	require.Equal(t, "propose", committer.getCurrentState())

	// test recovery from commit state (should transition to propose)
	// first to precommit, then to commit
	committer.state.Transition(precommitStage)
	committer.state.Transition(commitStage)
	require.Equal(t, "commit", committer.getCurrentState())

	committer.recoverToPropose(0)
	// should transition to propose since commit -> propose is valid in FSM
	require.Equal(t, "propose", committer.getCurrentState())
}

func TestAbort_CurrentHeight(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "db")
	walPath := filepath.Join(tempDir, "wal")

	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	defer wal.Close()

	// create 3PC committer
	committer := NewCommitter(database, "three-phase", wal, 5000)

	// set up a transaction at current height
	ctx := context.Background()
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}

	_, err = committer.Propose(ctx, proposeReq)
	require.NoError(t, err)

	// move to precommit state
	_, err = committer.Precommit(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, "precommit", committer.getCurrentState())

	// test abort for current height
	abortReq := &dto.AbortRequest{
		Height: 0,
		Reason: "Test abort",
	}

	resp, err := committer.Abort(ctx, abortReq)
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)

	// should be back in propose state after abort
	require.Equal(t, "propose", committer.getCurrentState())

	// should have the original data in WAL (can't overwrite existing entries)
	key, val, ok := wal.Get(0)
	require.True(t, ok)
	require.Equal(t, "test-key", key) // original data remains
	require.Equal(t, "tombstone", string(val), "Value should be tombstone after abort")

	value, err := database.Get("test-key")
	require.Error(t, err, "Original value should not exist in database after abort")
	require.Nil(t, value)
}

func TestAbort_FutureHeight(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "db")
	walPath := filepath.Join(tempDir, "wal")

	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	defer wal.Close()

	// create committer
	committer := NewCommitter(database, "three-phase", wal, 5000)

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

	_, _, ok := wal.Get(0)
	require.False(t, ok, "WAL should not have entry for current height without propose")
}

func TestAbort_PastHeight(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "db")
	walPath := filepath.Join(tempDir, "wal")

	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	defer wal.Close()

	// create committer and advance height
	committer := NewCommitter(database, "two-phase", wal, 5000)

	// complete a transaction to advance height
	ctx := context.Background()
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}

	_, err = committer.Propose(ctx, proposeReq)
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
	key, val, ok := wal.Get(0)
	require.True(t, ok, "WAL should have entry for past committed transaction")
	require.Equal(t, "test-key", key)
	require.Equal(t, "test-value", string(val), "Value should remain original (not tombstone) for past height")

	// check normal data is in db
	value, err := database.Get("test-key")
	require.NoError(t, err, "Data should exist in database for past committed transaction")
	require.Equal(t, "test-value", string(value))
}

func TestAbort_StateRecovery_3PC(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "db")
	walPath := filepath.Join(tempDir, "wal")

	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	defer wal.Close()

	// create 3PC committer
	committer := NewCommitter(database, "three-phase", wal, 5000)

	// set up transaction and move to precommit state
	ctx := context.Background()
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}

	_, err = committer.Propose(ctx, proposeReq)
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
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)

	// should be back in propose state
	require.Equal(t, "propose", committer.getCurrentState())

	// check wal
	key, val, ok := wal.Get(0)
	require.True(t, ok)
	require.Equal(t, "test-key", key)
	require.Equal(t, "tombstone", string(val), "Value should be tombstone after abort")
}

func TestAbort_StateRecovery_2PC(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "db")
	walPath := filepath.Join(tempDir, "wal")

	database, err := db.New(dbPath)
	require.NoError(t, err)
	defer database.Close()

	walConfig := gowal.Config{
		Dir:              walPath,
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	}

	wal, err := gowal.NewWAL(walConfig)
	require.NoError(t, err)
	defer wal.Close()

	// create 2PC committer
	committer := NewCommitter(database, "two-phase", wal, 5000)

	// set up transaction (in 2PC, we stay in propose state)
	ctx := context.Background()
	proposeReq := &dto.ProposeRequest{
		Height: 0,
		Key:    "test-key",
		Value:  []byte("test-value"),
	}

	_, err = committer.Propose(ctx, proposeReq)
	require.NoError(t, err)
	require.Equal(t, "propose", committer.getCurrentState())

	// test abort from propose state in 2PC
	abortReq := &dto.AbortRequest{
		Height: 0,
		Reason: "Test 2PC abort",
	}

	resp, err := committer.Abort(ctx, abortReq)
	require.NoError(t, err)
	require.Equal(t, dto.ResponseTypeAck, resp.ResponseType)

	// should remain in propose state
	require.Equal(t, "propose", committer.getCurrentState())

	// check wal
	key, val, ok := wal.Get(0)
	require.True(t, ok)
	require.Equal(t, "test-key", key)
	require.Equal(t, "tombstone", string(val), "Value should be tombstone after abort")
}
