package cache

import (
	"sync"
)

type msg struct {
	Key   string
	Value []byte
}

type Cache struct {
	store map[uint64]msg
	mu    sync.RWMutex
}

func New() *Cache {
	hashtable := make(map[uint64]msg)
	return &Cache{store: hashtable}
}

func (c *Cache) Set(index uint64, key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[index] = msg{key, value}
}

func (c *Cache) Get(index uint64) (string, []byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	message, ok := c.store[index]
	return message.Key, message.Value, ok
}
