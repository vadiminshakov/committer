package db

type Repository interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Close() error
}
