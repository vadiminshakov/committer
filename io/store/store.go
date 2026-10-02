package store

import (
	"context"
	stdErrors "errors"
	"os"

	"github.com/dgraph-io/badger/v4"
	"github.com/pkg/errors"
	"github.com/vadiminshakov/committer/v2/core/dto"
)

// Store persists committed key/value pairs in BadgerDB. It is the built-in
// resource of the committer binary.
type Store struct {
	db *badger.DB
}

// dbDirPerm is the directory permission for the Badger database directory.
const dbDirPerm = 0o755

// ErrNotFound returned when key does not exist in the store.
var ErrNotFound = errors.New("key not found")

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
				return errors.Wrap(err, "read badger item value")
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

// Open opens a state store. Owners of a WAL replay it themselves.
func Open(dbPath string) (*Store, error) {
	if dbPath == "" {
		return nil, errors.New("db path is empty")
	}

	if err := os.MkdirAll(dbPath, dbDirPerm); err != nil {
		return nil, errors.Wrap(err, "create badger directory")
	}

	opts := badger.DefaultOptions(dbPath)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, errors.Wrap(err, "open badger db")
	}

	return &Store{db: db}, nil
}

// Put stores the provided value for the key.
func (s *Store) Put(key string, value []byte) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}

	if err := s.db.Update(func(txn *badger.Txn) error {
		if value == nil {
			if err := txn.Delete([]byte(key)); err != nil && !stdErrors.Is(err, badger.ErrKeyNotFound) {
				return errors.Wrapf(err, "delete key %q", key)
			}

			return nil
		}

		if err := txn.Set([]byte(key), cloneBytes(value)); err != nil {
			return errors.Wrapf(err, "store key %q", key)
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "update store")
	}

	return nil
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

			return errors.Wrapf(err, "get key %q", key)
		}

		result, err = item.ValueCopy(nil)
		if err != nil {
			return errors.Wrapf(err, "copy value for key %q", key)
		}

		return nil
	})
	if err != nil {
		// ErrNotFound is a sentinel: callers compare it by identity.
		if stdErrors.Is(err, ErrNotFound) {
			return nil, err //nolint:wrapcheck
		}

		return nil, errors.Wrap(err, "read store")
	}

	return cloneBytes(result), nil
}

// Prepare votes on a key/value write. Badger applies a single write
// atomically, so there is nothing to reserve before commit.
func (s *Store) Prepare(_ context.Context, tx dto.Tx) error {
	if tx.Key == "" {
		return errors.New("key cannot be empty")
	}

	return nil
}

// Commit applies a key/value write. Repeating it is harmless.
func (s *Store) Commit(_ context.Context, tx dto.Tx) error {
	return s.Put(tx.Key, tx.Value)
}

// Abort is a no-op: Prepare reserves nothing.
func (s *Store) Abort(context.Context, uint64) error {
	return nil
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
