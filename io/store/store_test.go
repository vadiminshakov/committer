package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vadiminshakov/committer/io/wal"
	"github.com/vadiminshakov/gowal"
)

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
	s, state, err := New(w2, dbDir)
	require.NoError(t, err)
	defer s.Close()

	// 4. verify
	assert.Equal(t, height+1, state.Height)

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

	s, state, err := New(w2, dbDir)
	require.NoError(t, err)
	defer s.Close()

	// should NOT be applied to DB
	_, err = s.Get("key2")
	assert.Equal(t, ErrNotFound, err)

	// height should resume at 15 (incomplete)
	assert.Equal(t, height, state.Height)
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

	s, state, err := New(w2, dbDir)
	require.NoError(t, err)
	defer s.Close()

	// height should be 20+1 because it was resolved (Aborted)
	assert.Equal(t, height+1, state.Height)
}
