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

type heightState struct {
	maxPhase       string
	pendingPayload []byte
	decided        bool
}

func New(w *gowal.Wal) *Wal {
	return &Wal{w: w}
}

func (a *Wal) Write(key string, value []byte) error {
	if err := a.w.Write(gowal.Record{Index: a.w.CurrentIndex() + 1, Key: key, Value: value}); err != nil {
		return errors.Wrapf(err, "write wal record %q", key)
	}

	return nil
}

func (a *Wal) CurrentIndex() uint64 { return a.w.CurrentIndex() }
func (a *Wal) Close() error         { return a.w.Close() }

// Recover replays WAL entries, applies committed transactions, and reports the
// next unused height plus any last transaction that still lacks a decision.
func (a *Wal) Recover(applyFn func(key string, value []byte) error) (*RecoveryState, error) {
	states := make(map[uint64]*heightState)

	for record := range a.w.Iterator() {
		if err := applyRecord(states, record, applyFn); err != nil {
			return nil, err
		}
	}

	return summarizeRecovery(states), nil
}

// applyRecord folds one WAL record into the per-height recovery states.
func applyRecord(
	states map[uint64]*heightState,
	record gowal.Record,
	applyFn func(key string, value []byte) error,
) error {
	phase, height, ok := ParseKey(record.Key)
	if !ok {
		return nil
	}

	state, exists := states[height]
	if !exists {
		state = &heightState{}
		states[height] = state
	}

	switch phase {
	case PhaseKeyPrepared, PhaseKeyPrecommit:
		// A final decision fences the height. This also makes recovery
		// idempotent if a retry appended a duplicate phase record.
		if !state.decided {
			state.pendingPayload = record.Value
			state.maxPhase = phase
		}
	case PhaseKeyCommit:
		if state.decided {
			return nil
		}

		state.maxPhase = phase
		state.decided = true

		walTx, err := Decode(record.Value)
		if err != nil {
			return errors.Wrapf(err, "decode wal tx at idx %d", record.Index)
		}

		if err := applyFn(walTx.Key, walTx.Value); err != nil {
			return errors.Wrapf(err, "apply committed tx at idx %d", record.Index)
		}
	case PhaseKeyAbort:
		if state.decided {
			return nil
		}

		state.maxPhase = phase
		state.decided = true
		state.pendingPayload = nil
	}

	return nil
}

// summarizeRecovery derives the recovery result from per-height states.
func summarizeRecovery(states map[uint64]*heightState) *RecoveryState {
	result := &RecoveryState{Decisions: make(map[uint64]string)}
	if len(states) == 0 {
		return result
	}

	var maxHeight uint64
	for height, state := range states {
		if height > maxHeight {
			maxHeight = height
		}

		if state.maxPhase == PhaseKeyCommit || state.maxPhase == PhaseKeyAbort {
			result.Decisions[height] = state.maxPhase
		}
	}

	result.NextHeight = maxHeight + 1
	latest := states[maxHeight]

	switch latest.maxPhase {
	case PhaseKeyCommit, PhaseKeyAbort:
		// the last transaction is resolved; there is nothing else to restore.
	case PhaseKeyPrepared, PhaseKeyPrecommit:
		result.Unresolved = &UnresolvedTransaction{
			Height:  maxHeight,
			Phase:   latest.maxPhase,
			Payload: latest.pendingPayload,
		}
	}

	return result
}
