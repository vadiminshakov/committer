package voteslog

import (
	"github.com/stretchr/testify/require"
	"github.com/vadiminshakov/committer/core/entity"
	"os"
	"strconv"
	"testing"
)

func TestSetGet(t *testing.T) {
	log, err := NewOnDiskLog("./testlogdata")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		require.NoError(t, log.Set(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))))
	}

	for i := 0; i < 10; i++ {
		key, value, ok := log.Get(uint64(i))
		require.True(t, ok)
		require.Equal(t, "key"+strconv.Itoa(i), key)
		require.Equal(t, "value"+strconv.Itoa(i), string(value))
	}

	require.NoError(t, os.RemoveAll("./testlogdata"))
}

func TestLoadIndexMsg(t *testing.T) {
	log, err := NewOnDiskLog("./testlogdata")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		require.NoError(t, log.Set(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))))
	}

	stat, err := log.msgs.Stat()
	require.NoError(t, err)

	index, err := loadIndexesMsg(log.msgs, stat)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		require.Equal(t, "key"+strconv.Itoa(i), index[uint64(i)].Key)
		require.Equal(t, "value"+strconv.Itoa(i), string(index[uint64(i)].Value))
	}

	require.NoError(t, os.RemoveAll("./testlogdata"))
}

func TestSegmentRotationForMsgs(t *testing.T) {
	log, err := NewOnDiskLog("./testlogdata")
	require.NoError(t, err)

	for i := 0; i < 49; i++ {
		require.NoError(t, log.Set(uint64(i), "key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))))
	}

	stat, err := log.msgs.Stat()
	require.NoError(t, err)

	index, err := loadIndexesMsg(log.msgs, stat)
	require.NoError(t, err)

	for i := 41; i < 49; i++ {
		require.Equal(t, "key"+strconv.Itoa(i), index[uint64(i)].Key)
		require.Equal(t, "value"+strconv.Itoa(i), string(index[uint64(i)].Value))
	}

	require.NoError(t, os.RemoveAll("./testlogdata"))
}

func TestLoadIndexVotes(t *testing.T) {
	log, err := NewOnDiskLog("./testlogdata")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		require.NoError(t, log.SetVotes(uint64(i), []*entity.Vote{{"node" + strconv.Itoa(i), true}}))
	}

	stat, err := log.votes.Stat()
	require.NoError(t, err)

	index, err := loadIndexesVotes(log.votes, stat)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		require.Equal(t, entity.Vote{"node" + strconv.Itoa(i), true}, *index[uint64(i)].Votes[0])
	}

	require.NoError(t, os.RemoveAll("./testlogdata"))
}
