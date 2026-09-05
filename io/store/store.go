package store

import (
	stdErrors "errors"
	"os"

	"github.com/dgraph-io/badger/v4"
	"github.com/pkg/errors"
	"github.com/vadiminshakov/committer/io/wal"
)

// ErrNotFound returned when key does not exist in the store.
var ErrNotFound = errors.New("key not found")

// Store persists committed key/value pairs in BadgerDB and reconstructs them from WAL on startup.
type Store struct {
	db *badger.DB
}

// Snapshot returns a shallow copy of the current state.
func (s *Store) Snapshot() map[string][]byte {
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
	count := 0
	_ = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})

	return count
}

// Open opens a state store without replaying a journal. The owner of a deep
// transaction lifecycle can use this form and invoke journal recovery itself.
func Open(dbPath string) (*Store, error) {
	if dbPath == "" {
		return nil, errors.New("db path is empty")
	}

	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		return nil, errors.Wrap(err, "create badger directory")
	}

	opts := badger.DefaultOptions(dbPath)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, errors.Wrap(err, "open badger db")
	}
	return &Store{db: db}, nil
}

// New creates a WAL-backed store and reconstructs state from WAL entries.
// Cohort construction and existing callers retain this convenience behavior;
// coordinator construction uses Open so its transaction lifecycle owns replay.
func New(w *wal.Wal, dbPath string) (*Store, *wal.RecoveryState, error) {
	if w == nil {
		return nil, nil, errors.New("wal is nil")
	}

	s, err := Open(dbPath)
	if err != nil {
		return nil, nil, err
	}

	recovery, err := w.Recover(s.Put)
	if err != nil {
		_ = s.Close()
		return nil, nil, err
	}

	return s, recovery, nil
}

// Put stores the provided value for the key.
func (s *Store) Put(key string, value []byte) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}

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

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}

	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
