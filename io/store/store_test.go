package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vadiminshakov/committer/v2/core/dto"
	"github.com/vadiminshakov/committer/v2/io/wal"
	"github.com/vadiminshakov/gowal"
)

func TestOpenCreatesStoreWithoutRequiringJournalRecovery(t *testing.T) {
	s, err := Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	require.NoError(t, s.Put("key", []byte("value")))
	value, err := s.Get("key")
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
}

func TestStore_Recovery_Commit(t *testing.T) {
	walDir := filepath.Join(os.TempDir(), "wal_commit")
	dbDir := filepath.Join(os.TempDir(), "db_commit")

	defer os.RemoveAll(walDir)
	defer os.RemoveAll(dbDir)

	// 1. setup WAL
	w, err := gowal.NewWAL(gowal.Config{
		Dir:              walDir,
		Prefix:           "wal_",
		SegmentThreshold: 1024 * 1024,
		MaxSegments:      10,
	})
	require.NoError(t, err)

	// 2. write prepared then commit sequentially (indices 1, 2)
	height := uint64(10)
	tx := wal.Tx{Key: "key1", Value: []byte("value1")}
	encoded, _ := wal.Encode(tx)

	err = w.Write(gowal.Record{Index: 1, Key: wal.PreparedKey(height), Value: encoded})
	require.NoError(t, err)

	err = w.Write(gowal.Record{Index: 2, Key: wal.CommitKey(height), Value: encoded})
	require.NoError(t, err)

	w.Close()

	w2, err := gowal.NewWAL(gowal.Config{Dir: walDir, Prefix: "wal_", SegmentThreshold: 1024 * 1024, MaxSegments: 10})
	require.NoError(t, err)

	defer w2.Close()

	// 3. recover
	s, state, err := openRecovered(wal.New(w2), dbDir)
	require.NoError(t, err)

	defer s.Close()

	// 4. verify
	assert.Equal(t, height+1, state.NextHeight)
	require.NotNil(t, state.LastDecided)
	assert.Equal(t, height, state.LastDecided.Height)
	assert.Equal(t, wal.PhaseKeyCommit, state.LastDecided.Phase)
	assert.Equal(t, encoded, state.LastDecided.Payload)

	val, err := s.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value1"), val)
}

func TestStore_Recovery_PreparedOnly(t *testing.T) {
	walDir := filepath.Join(os.TempDir(), "wal_prepared")
	dbDir := filepath.Join(os.TempDir(), "db_prepared")

	defer os.RemoveAll(walDir)
	defer os.RemoveAll(dbDir)

	w, err := gowal.NewWAL(gowal.Config{
		Dir:              walDir,
		Prefix:           "wal_",
		SegmentThreshold: 1024 * 1024,
		MaxSegments:      10,
	})
	require.NoError(t, err)

	height := uint64(15)
	tx := wal.Tx{Key: "key2", Value: []byte("value2")}
	encoded, _ := wal.Encode(tx)

	err = w.Write(gowal.Record{Index: 1, Key: wal.PreparedKey(height), Value: encoded})
	require.NoError(t, err)
	w.Close()

	w2, err := gowal.NewWAL(gowal.Config{Dir: walDir, Prefix: "wal_", SegmentThreshold: 1024 * 1024, MaxSegments: 10})
	require.NoError(t, err)

	defer w2.Close()

	s, state, err := openRecovered(wal.New(w2), dbDir)
	require.NoError(t, err)

	defer s.Close()

	// should NOT be applied to DB
	_, err = s.Get("key2")
	assert.Equal(t, ErrNotFound, err)

	assert.Equal(t, height+1, state.NextHeight)
	assert.Equal(t, height, state.Unresolved.Height)
	assert.Equal(t, wal.PhaseKeyPrepared, state.Unresolved.Phase)
	assert.Equal(t, encoded, state.Unresolved.Payload)
}

func TestStore_Recovery_Abort(t *testing.T) {
	walDir := filepath.Join(os.TempDir(), "wal_abort")
	dbDir := filepath.Join(os.TempDir(), "db_abort")

	defer os.RemoveAll(walDir)
	defer os.RemoveAll(dbDir)

	w, err := gowal.NewWAL(gowal.Config{
		Dir:              walDir,
		Prefix:           "wal_",
		SegmentThreshold: 1024 * 1024,
		MaxSegments:      10,
	})
	require.NoError(t, err)

	height := uint64(20)

	// write abort at index 1
	err = w.Write(gowal.Record{Index: 1, Key: wal.AbortKey(height)})
	require.NoError(t, err)
	w.Close()

	w2, err := gowal.NewWAL(gowal.Config{Dir: walDir, Prefix: "wal_", SegmentThreshold: 1024 * 1024, MaxSegments: 10})
	require.NoError(t, err)

	defer w2.Close()

	s, state, err := openRecovered(wal.New(w2), dbDir)
	require.NoError(t, err)

	defer s.Close()

	// height should be 20+1 because it was resolved (Aborted)
	assert.Equal(t, height+1, state.NextHeight)
	require.NotNil(t, state.LastDecided)
	assert.Equal(t, wal.PhaseKeyAbort, state.LastDecided.Phase)
	assert.Nil(t, state.LastDecided.Payload)
}

func TestStoreAsResource(t *testing.T) {
	s, err := Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	ctx := context.Background()
	tx := dto.Tx{Key: "k", Value: []byte("v")}

	require.Error(t, s.Prepare(ctx, dto.Tx{}))
	require.NoError(t, s.Prepare(ctx, tx))
	require.NoError(t, s.Commit(ctx, tx))
	require.NoError(t, s.Commit(ctx, tx), "commit must be idempotent")
	require.NoError(t, s.Abort(ctx, 42), "abort of an unknown height is a no-op")

	value, err := s.Get("k")
	require.NoError(t, err)
	require.Equal(t, []byte("v"), value)
}

// openRecovered opens a store and replays every committed WAL record into it.
func openRecovered(journal *wal.Wal, dbPath string) (*Store, *wal.RecoveryState, error) {
	s, err := Open(dbPath)
	if err != nil {
		return nil, nil, err
	}

	state, err := journal.Recover(s.Put)
	if err != nil {
		_ = s.Close()

		return nil, nil, fmt.Errorf("recover: %w", err)
	}

	return s, state, nil
}
