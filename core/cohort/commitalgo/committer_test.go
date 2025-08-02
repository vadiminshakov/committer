package commitalgo

import (
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

	// Test: create committer without hooks (should use default)
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

	// Test: create committer with custom hooks
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

	// Test: create committer and add hooks dynamically
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
