package store

import (
	stdErrors "errors"
	"os"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/pkg/errors"
	"github.com/vadiminshakov/committer/core/walrecord"
	"github.com/vadiminshakov/gowal"
)

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
	// Height is the current protocol height (next proposal should use this height).
	Height uint64
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
		hasProto       bool
		maxProtoHeight uint64
		maxProtoStatus string // last seen status for maxProtoHeight
	)

	for msg := range s.wal.Iterator() {
		if msg.Key == "" || msg.Key == "skip" {
			continue
		}

		// check if it is a system/protocol key
		if strings.HasPrefix(msg.Key, "__tx:") {
			hasProto = true
			height := msg.Idx / walrecord.Stride

			// track max proto height and its status
			if height > maxProtoHeight {
				maxProtoHeight = height
				maxProtoStatus = msg.Key
			} else if height == maxProtoHeight {
				// if multiple messages for same height, commit/abort override prepared/precommit as "final" status
				if msg.Key == walrecord.KeyCommit || msg.Key == walrecord.KeyAbort {
					maxProtoStatus = msg.Key
				}
			}

			// apply strictly on commit
			if msg.Key == walrecord.KeyCommit {
				walTx, err := walrecord.Decode(msg.Value)
				if err != nil {
					return nil, errors.Wrapf(err, "decode wal tx at idx %d", msg.Idx)
				}
				if err := s.Put(walTx.Key, walTx.Value); err != nil {
					return nil, errors.Wrapf(err, "apply committed tx at idx %d", msg.Idx)
				}
			}
		}
	}

	var height uint64
	if hasProto {
		if maxProtoStatus == walrecord.KeyCommit || maxProtoStatus == walrecord.KeyAbort {
			height = maxProtoHeight + 1
		} else {
			// incomplete transaction at maxProtoHeight
			height = maxProtoHeight
		}
	}

	return &RecoveryState{Height: height}, nil
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}

	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
