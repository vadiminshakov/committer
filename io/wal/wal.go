package wal

import (
	"github.com/pkg/errors"
	"github.com/vadiminshakov/gowal"
)

// RecoveryState contains information extracted from WAL during startup.
type RecoveryState struct {
	// Height is the current protocol height (next proposal should use this height).
	Height uint64
	// PendingPayload is the encoded transaction payload for an incomplete transaction
	// (nil if no incomplete transaction exists).
	PendingPayload []byte
}

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

type heightState struct {
	maxPhase       string
	pendingPayload []byte
}

// Recover replays WAL entries and calls applyFn for each committed transaction.
// It returns the recovered state including the current height and any pending payload.
func (a *Wal) Recover(applyFn func(key string, value []byte) error) (*RecoveryState, error) {
	states := make(map[uint64]*heightState)

	for rec := range a.w.Iterator() {
		phase, height, ok := ParseKey(rec.Key)
		if !ok {
			continue
		}

		st, exists := states[height]
		if !exists {
			st = &heightState{}
			states[height] = st
		}

		switch phase {
		case PhaseKeyPrepared, PhaseKeyPrecommit:
			st.pendingPayload = rec.Value
			st.maxPhase = phase
		case PhaseKeyCommit:
			st.maxPhase = phase
			walTx, err := Decode(rec.Value)
			if err != nil {
				return nil, errors.Wrapf(err, "decode wal tx at idx %d", rec.Index)
			}
			if err := applyFn(walTx.Key, walTx.Value); err != nil {
				return nil, errors.Wrapf(err, "apply committed tx at idx %d", rec.Index)
			}
		case PhaseKeyAbort:
			st.maxPhase = phase
			st.pendingPayload = nil
		}
	}

	if len(states) == 0 {
		return &RecoveryState{}, nil
	}

	var maxHeight uint64
	for h := range states {
		if h > maxHeight {
			maxHeight = h
		}
	}

	st := states[maxHeight]
	result := &RecoveryState{}

	switch st.maxPhase {
	case PhaseKeyCommit, PhaseKeyAbort:
		result.Height = maxHeight + 1
	default:
		result.Height = maxHeight
		result.PendingPayload = st.pendingPayload
	}

	return result, nil
}
