package voteslog

import (
	"github.com/vadiminshakov/committer/core/dto"
	"sync"
)

type InmemVotesLog struct {
	kv      map[uint64]msg
	votes   map[uint64][]*dto.Vote
	muKv    sync.RWMutex
	muVotes sync.RWMutex
}

func NewInmemLog() *InmemVotesLog {
	kvstore, votesstore := make(map[uint64]msg), make(map[uint64][]*dto.Vote)
	return &InmemVotesLog{kv: kvstore, votes: votesstore}
}

func (c *InmemVotesLog) Set(index uint64, key string, value []byte) error {
	c.muKv.Lock()
	defer c.muKv.Unlock()
	c.kv[index] = msg{index, key, value}

	return nil
}

func (c *InmemVotesLog) Get(index uint64) (string, []byte, bool) {
	c.muKv.RLock()
	defer c.muKv.RUnlock()
	message, ok := c.kv[index]
	return message.Key, message.Value, ok
}

func (c *InmemVotesLog) SetVotes(index uint64, votes []*dto.Vote) error {
	c.muVotes.Lock()
	defer c.muVotes.Unlock()
	c.votes[index] = append(c.votes[index], votes...)

	return nil
}

func (c *InmemVotesLog) GetVotes(index uint64) []*dto.Vote {
	c.muVotes.RLock()
	defer c.muVotes.RUnlock()
	return c.votes[index]
}

func (c *InmemVotesLog) Close() error {
	return nil
}
