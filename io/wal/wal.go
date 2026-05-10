package wal

import "github.com/vadiminshakov/gowal"

// Wal wraps *gowal.Wal and exposes a primitive-typed interface,
// keeping gowal details out of the core domain packages.
type Wal struct {
	w *gowal.Wal
}

func New(w *gowal.Wal) *Wal {
	return &Wal{w: w}
}

func (a *Wal) Write(key string, value []byte) error {
	return a.w.Write(gowal.Record{Index: a.w.CurrentIndex() + 1, Key: key, Value: value})
}

func (a *Wal) CurrentIndex() uint64 { return a.w.CurrentIndex() }
func (a *Wal) Close() error          { return a.w.Close() }
