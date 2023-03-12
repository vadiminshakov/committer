package db

type Database interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Close() error
}
