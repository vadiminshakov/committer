// Package store provides persistent storage capabilities using BadgerDB.
//
// This package handles state persistence and recovery from write-ahead logs,
// ensuring data consistency across system restarts.
package store

import (
	"bytes"
	stdErrors "errors"
	"os"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/pkg/errors"
	"github.com/vadiminshakov/gowal"
)

// tombstoneMarker is used to mark aborted transactions in the WAL.
const tombstoneMarker = "tombstone"

// ErrNotFound returned when key does not exist in the store.
var ErrNotFound = errors.New("key not found")

// Store persists committed key/value pairs in BadgerDB and reconstructs them from WAL on startup.
type Store struct {
	wal *gowal.Wal
	db  *badger.DB
	mu  sync.RWMutex
}

// Snapshot returns a shallow copy of the current state.
func (s *Store) Snapshot() map[string][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshot := make(map[string][]byte)
	_ = s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			if err := item.Value(func(val []byte) error {
				snapshot[string(key)] = cloneBytes(val)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})

	return snapshot
}

// Size returns current number of keys in the store.
func (s *Store) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	_ = s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})

	return count
}

// RecoveryState contains information extracted from WAL during startup.
type RecoveryState struct {
	// NextHeight is the next transaction height to use after recovery.
	NextHeight uint64
}

// New creates a new WAL-backed store and reconstructs state from WAL entries using BadgerDB.
func New(wal *gowal.Wal, dbPath string) (*Store, *RecoveryState, error) {
	if wal == nil {
		return nil, nil, errors.New("wal is nil")
	}
	if dbPath == "" {
		return nil, nil, errors.New("db path is empty")
	}

	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		return nil, nil, errors.Wrap(err, "create badger directory")
	}

	opts := badger.DefaultOptions(dbPath)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, nil, errors.Wrap(err, "open badger db")
	}

	state := &Store{
		wal: wal,
		db:  db,
	}

	recovery, err := state.recover()
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}

	return state, recovery, nil
}

// Put stores the provided value for the key.
func (s *Store) Put(key string, value []byte) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Update(func(txn *badger.Txn) error {
		if value == nil {
			if err := txn.Delete([]byte(key)); err != nil && !stdErrors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			return nil
		}
		return txn.Set([]byte(key), cloneBytes(value))
	})
}

// Get retrieves value by key. Returns ErrNotFound if key does not exist.
func (s *Store) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if stdErrors.Is(err, badger.ErrKeyNotFound) {
				return ErrNotFound
			}
			return err
		}
		result, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, err
	}

	return cloneBytes(result), nil
}

// Close closes the underlying Badger database.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) recover() (*RecoveryState, error) {
	var (
		maxIndex   uint64
		hasEntries bool
	)

	for msg := range s.wal.Iterator() {
		hasEntries = true
		if msg.Idx > maxIndex {
			maxIndex = msg.Idx
		}

		if msg.Key == "" || msg.Key == "skip" {
			continue
		}

		if err := s.db.Update(func(txn *badger.Txn) error {
			key := []byte(msg.Key)
			switch {
			case bytes.Equal(msg.Value, []byte(tombstoneMarker)):
				if err := txn.Delete(key); err != nil && !stdErrors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
			case msg.Value == nil:
				return nil
			default:
				if err := txn.Set(key, cloneBytes(msg.Value)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return nil, errors.Wrap(err, "apply wal entry")
		}
	}

	nextHeight := uint64(0)
	if hasEntries {
		nextHeight = maxIndex + 1
	}

	return &RecoveryState{NextHeight: nextHeight}, nil
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}

	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
