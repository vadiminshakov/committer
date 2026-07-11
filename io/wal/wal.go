package wal

import (
	"github.com/pkg/errors"
	"github.com/vadiminshakov/gowal"
)

// UnresolvedTransaction describes the last transaction when WAL contains a
// prepared or precommit record but no final commit or abort decision.
type UnresolvedTransaction struct {
	Height  uint64
	Phase   string
	Payload []byte
}

// RecoveryState contains facts reconstructed from WAL during startup.
type RecoveryState struct {
	// NextHeight is the first protocol height not represented in WAL.
	NextHeight uint64
	// Unresolved is non-nil when the last transaction has no final decision.
	Unresolved *UnresolvedTransaction
	// Decisions maps every decided height to its final outcome
	// (PhaseKeyCommit or PhaseKeyAbort). Used to answer decision requests
	// from in-doubt cohorts and to catch up lagging ones.
	Decisions map[uint64]string
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
func (a *Wal) Close() error         { return a.w.Close() }

type heightState struct {
	maxPhase       string
	pendingPayload []byte
}

// Recover replays WAL entries, applies committed transactions, and reports the
// next unused height plus any last transaction that still lacks a decision.
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

	result := &RecoveryState{Decisions: make(map[uint64]string)}
	if len(states) == 0 {
		return result, nil
	}

	var maxHeight uint64
	for h, st := range states {
		if h > maxHeight {
			maxHeight = h
		}
		if st.maxPhase == PhaseKeyCommit || st.maxPhase == PhaseKeyAbort {
			result.Decisions[h] = st.maxPhase
		}
	}

	result.NextHeight = maxHeight + 1
	st := states[maxHeight]

	switch st.maxPhase {
	case PhaseKeyCommit, PhaseKeyAbort:
		// the last transaction is resolved; there is nothing else to restore.
	case PhaseKeyPrepared, PhaseKeyPrecommit:
		result.Unresolved = &UnresolvedTransaction{
			Height:  maxHeight,
			Phase:   st.maxPhase,
			Payload: st.pendingPayload,
		}
	}

	return result, nil
}
