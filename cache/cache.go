package cache

import (
	"github.com/vadiminshakov/committer/entity"
	"sync"
)

type msg struct {
	Key   string
	Value []byte
}

type Cache struct {
	kv      map[uint64]msg
	votes   map[uint64][]*entity.Vote
	muKv    sync.RWMutex
	muVotes sync.RWMutex
}

func New() *Cache {
	kvstore, votesstore := make(map[uint64]msg), make(map[uint64][]*entity.Vote)
	return &Cache{kv: kvstore, votes: votesstore}
}

func (c *Cache) Set(index uint64, key string, value []byte) {
	c.muKv.Lock()
	defer c.muKv.Unlock()
	c.kv[index] = msg{key, value}
}

func (c *Cache) Get(index uint64) (string, []byte, bool) {
	c.muKv.RLock()
	defer c.muKv.RUnlock()
	message, ok := c.kv[index]
	return message.Key, message.Value, ok
}

func (c *Cache) Delete(index uint64) {
	c.muKv.Lock()
	delete(c.kv, index)
	c.muKv.Unlock()
}

func (c *Cache) SetVotes(index uint64, votes []*entity.Vote) {
	c.muVotes.Lock()
	defer c.muVotes.Unlock()
	c.votes[index] = append(c.votes[index], votes...)
}

func (c *Cache) GetVotes(index uint64) []*entity.Vote {
	c.muVotes.RLock()
	defer c.muVotes.RUnlock()
	return c.votes[index]
}

func (c *Cache) DelVotes(index uint64) {
	c.muVotes.Lock()
	delete(c.votes, index)
	c.muVotes.Unlock()
}
