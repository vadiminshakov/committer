package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vadiminshakov/gowal"
)

func openTestWAL(t *testing.T) *Wal {
	w, err := gowal.NewWAL(gowal.Config{
		Dir:              t.TempDir(),
		Prefix:           "test",
		SegmentThreshold: 1024,
		MaxSegments:      10,
		IsInSyncDiskMode: false,
	})
	require.NoError(t, err)
	t.Cleanup(func() { w.Close() })

	return New(w)
}

func mustEncode(t *testing.T, key, value string) []byte {
	payload, err := Encode(Tx{Key: key, Value: []byte(value)})
	require.NoError(t, err)
	return payload
}

func TestRecover_Empty(t *testing.T) {
	w := openTestWAL(t)

	rec, err := w.Recover(func(key string, value []byte) error { return nil })
	require.NoError(t, err)
	require.Equal(t, uint64(0), rec.NextHeight)
	require.Nil(t, rec.Unresolved)
	require.Empty(t, rec.Decisions)
}

func TestRecover_DecisionsAndPending(t *testing.T) {
	w := openTestWAL(t)

	// height 0: committed, height 1: aborted, height 2: prepared (in doubt)
	require.NoError(t, w.Write(PreparedKey(0), mustEncode(t, "k0", "v0")))
	require.NoError(t, w.Write(CommitKey(0), mustEncode(t, "k0", "v0")))
	require.NoError(t, w.Write(PreparedKey(1), mustEncode(t, "k1", "v1")))
	require.NoError(t, w.Write(AbortKey(1), nil))
	pending := mustEncode(t, "k2", "v2")
	require.NoError(t, w.Write(PreparedKey(2), pending))
	require.NoError(t, w.Write(PrecommitKey(2), pending))

	applied := make(map[string]string)
	rec, err := w.Recover(func(key string, value []byte) error {
		applied[key] = string(value)
		return nil
	})
	require.NoError(t, err)

	// only the committed transaction is applied
	require.Equal(t, map[string]string{"k0": "v0"}, applied)

	// next height is unambiguous; the in-doubt transaction carries its own state
	require.Equal(t, uint64(3), rec.NextHeight)
	require.Equal(t, &UnresolvedTransaction{
		Height:  2,
		Phase:   PhaseKeyPrecommit,
		Payload: pending,
	}, rec.Unresolved)

	// every resolved height has a recorded decision
	require.Equal(t, map[uint64]string{
		0: PhaseKeyCommit,
		1: PhaseKeyAbort,
	}, rec.Decisions)
}

func TestRecover_AbortAdvancesHeight(t *testing.T) {
	w := openTestWAL(t)

	require.NoError(t, w.Write(PreparedKey(0), mustEncode(t, "k0", "v0")))
	require.NoError(t, w.Write(AbortKey(0), nil))

	rec, err := w.Recover(func(key string, value []byte) error { return nil })
	require.NoError(t, err)

	// an aborted height is consumed, matching runtime behaviour
	require.Equal(t, uint64(1), rec.NextHeight)
	require.Nil(t, rec.Unresolved)
	require.Equal(t, map[uint64]string{0: PhaseKeyAbort}, rec.Decisions)
}
