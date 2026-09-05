package coordinator

import (
	"errors"
	"fmt"
	"sync"

	"github.com/vadiminshakov/committer/core/dto"
	iowal "github.com/vadiminshakov/committer/io/wal"
)

var (
	// ErrInvalidTransaction reports a request rejected before any durable
	// transaction record is written.
	ErrInvalidTransaction = errors.New("invalid transaction")
	// ErrCoordinatorNotReady reports that an unresolved or fenced transaction
	// prevents the lifecycle from accepting another transaction.
	ErrCoordinatorNotReady = errors.New("coordinator is not ready")
)

// CommittedNotAppliedError reports that COMMIT is durable but its mutation is
// not yet reflected in the local store. The coordinator must remain fenced.
type CommittedNotAppliedError struct {
	Height uint64
	cause  error
}

func (e *CommittedNotAppliedError) Error() string {
	return fmt.Sprintf("transaction at height %d is committed but not applied: %v", e.Height, e.cause)
}

func (e *CommittedNotAppliedError) Unwrap() error {
	return e.cause
}

type lifecycleWAL interface {
	Write(key string, value []byte) error
	Recover(applyFn func(key string, value []byte) error) (*iowal.RecoveryState, error)
}

type lifecycleStore interface {
	Put(key string, value []byte) error
}

type lifecyclePhase uint8

const (
	lifecycleReady lifecyclePhase = iota
	lifecyclePrepared
	lifecyclePrecommitted
	lifecycleFenced
	lifecycleFailed
)

// transactionLifecycle owns durable state and legal transitions for the one
// transaction a coordinator may have in flight.
type transactionLifecycle struct {
	mu sync.RWMutex

	protocol dto.Protocol
	wal      lifecycleWAL
	store    lifecycleStore

	height         uint64
	phase          lifecyclePhase
	pendingPayload []byte
	decisions      map[uint64]dto.Outcome
}

func newTransactionLifecycle(
	protocol dto.Protocol,
	wal lifecycleWAL,
	store lifecycleStore,
) (*transactionLifecycle, *dto.FinalDecision, error) {
	if protocol != dto.ProtocolTwoPhase && protocol != dto.ProtocolThreePhase {
		return nil, nil, fmt.Errorf("unsupported transaction protocol %d", protocol)
	}
	if wal == nil {
		return nil, nil, fmt.Errorf("transaction WAL is nil")
	}
	if store == nil {
		return nil, nil, fmt.Errorf("transaction store is nil")
	}

	recovery, err := wal.Recover(store.Put)
	if err != nil {
		return nil, nil, fmt.Errorf("recover transaction WAL: %w", err)
	}
	if recovery == nil {
		return nil, nil, fmt.Errorf("recover transaction WAL: recovery state is nil")
	}
	if recovery.Unresolved != nil {
		if recovery.NextHeight == 0 || recovery.Unresolved.Height != recovery.NextHeight-1 {
			return nil, nil, fmt.Errorf(
				"unresolved transaction height %d must immediately precede next height %d",
				recovery.Unresolved.Height,
				recovery.NextHeight,
			)
		}
		if _, decided := recovery.Decisions[recovery.Unresolved.Height]; decided {
			return nil, nil, fmt.Errorf(
				"transaction at height %d is both unresolved and final",
				recovery.Unresolved.Height,
			)
		}
		tx, err := iowal.Decode(recovery.Unresolved.Payload)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"decode unresolved transaction at height %d: %w",
				recovery.Unresolved.Height,
				err,
			)
		}
		if tx.Key == "" {
			return nil, nil, fmt.Errorf(
				"unresolved transaction at height %d has empty transaction key",
				recovery.Unresolved.Height,
			)
		}
	}

	lifecycle := &transactionLifecycle{
		protocol:  protocol,
		wal:       wal,
		store:     store,
		height:    recovery.NextHeight,
		decisions: make(map[uint64]dto.Outcome, len(recovery.Decisions)),
	}
	for height, phase := range recovery.Decisions {
		if height >= recovery.NextHeight {
			return nil, nil, fmt.Errorf(
				"recovered decision height %d is not below next height %d",
				height,
				recovery.NextHeight,
			)
		}
		switch phase {
		case iowal.PhaseKeyCommit:
			lifecycle.decisions[height] = dto.OutcomeCommit
		case iowal.PhaseKeyAbort:
			lifecycle.decisions[height] = dto.OutcomeAbort
		default:
			return nil, nil, fmt.Errorf("invalid recovered decision %q at height %d", phase, height)
		}
	}

	if recovery.Unresolved == nil {
		return lifecycle, nil, nil
	}

	unresolved := recovery.Unresolved
	lifecycle.height = unresolved.Height
	lifecycle.pendingPayload = append([]byte(nil), unresolved.Payload...)
	switch unresolved.Phase {
	case iowal.PhaseKeyPrepared:
		lifecycle.phase = lifecyclePrepared
		decision, err := lifecycle.Abort()
		if err != nil {
			return nil, nil, fmt.Errorf("recover prepared transaction at height %d: %w", unresolved.Height, err)
		}
		return lifecycle, &decision, nil
	case iowal.PhaseKeyPrecommit:
		if protocol != dto.ProtocolThreePhase {
			return nil, nil, fmt.Errorf("precommit recovery at height %d requires three-phase protocol", unresolved.Height)
		}
		tx, err := iowal.Decode(lifecycle.pendingPayload)
		if err != nil {
			return nil, nil, fmt.Errorf("decode recovered precommit at height %d: %w", unresolved.Height, err)
		}
		if tx.Key == "" {
			return nil, nil, fmt.Errorf("recovered precommit at height %d has empty transaction key", unresolved.Height)
		}
		lifecycle.phase = lifecyclePrecommitted
		decision, err := lifecycle.Commit()
		if err != nil {
			return nil, nil, fmt.Errorf("recover precommitted transaction at height %d: %w", unresolved.Height, err)
		}
		decision.RequirePrecommit = true
		return lifecycle, &decision, nil
	default:
		return nil, nil, fmt.Errorf("unsupported unresolved phase %q at height %d", unresolved.Phase, unresolved.Height)
	}
}

func (l *transactionLifecycle) Prepare(tx dto.Transaction) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if tx.Key == "" {
		return 0, fmt.Errorf("%w: transaction key is empty", ErrInvalidTransaction)
	}
	if l.phase != lifecycleReady {
		return 0, fmt.Errorf("%w: transaction at height %d is not resolved", ErrCoordinatorNotReady, l.height)
	}

	payload, err := iowal.Encode(iowal.Tx{Key: tx.Key, Value: tx.Value})
	if err != nil {
		return 0, fmt.Errorf("encode transaction: %w", err)
	}
	if err := l.wal.Write(iowal.PreparedKey(l.height), payload); err != nil {
		l.pendingPayload = payload
		l.phase = lifecycleFailed
		return 0, fmt.Errorf("write prepared transaction at height %d: %w", l.height, err)
	}

	l.pendingPayload = payload
	l.phase = lifecyclePrepared
	return l.height, nil
}

func (l *transactionLifecycle) Precommit() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.protocol != dto.ProtocolThreePhase || l.phase != lifecyclePrepared {
		return fmt.Errorf("precommit is not legal at height %d", l.height)
	}
	if err := l.wal.Write(iowal.PrecommitKey(l.height), l.pendingPayload); err != nil {
		l.phase = lifecycleFailed
		return fmt.Errorf("write precommit at height %d: %w", l.height, err)
	}

	l.phase = lifecyclePrecommitted
	return nil
}

func (l *transactionLifecycle) Commit() (dto.FinalDecision, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	legal := l.protocol == dto.ProtocolTwoPhase && l.phase == lifecyclePrepared ||
		l.protocol == dto.ProtocolThreePhase && l.phase == lifecyclePrecommitted
	if !legal {
		return dto.FinalDecision{}, fmt.Errorf("commit is not legal at height %d", l.height)
	}

	height := l.height
	if err := l.wal.Write(iowal.CommitKey(height), l.pendingPayload); err != nil {
		l.phase = lifecycleFailed
		return dto.FinalDecision{}, fmt.Errorf("write commit decision at height %d: %w", height, err)
	}

	decision := dto.FinalDecision{Height: height, Outcome: dto.OutcomeCommit}
	l.decisions[height] = dto.OutcomeCommit
	l.phase = lifecycleFenced

	tx, err := iowal.Decode(l.pendingPayload)
	if err != nil {
		return decision, &CommittedNotAppliedError{Height: height, cause: fmt.Errorf("decode transaction: %w", err)}
	}
	if err := l.store.Put(tx.Key, tx.Value); err != nil {
		return decision, &CommittedNotAppliedError{Height: height, cause: err}
	}

	l.pendingPayload = nil
	l.phase = lifecycleReady
	l.height++
	return decision, nil
}

func (l *transactionLifecycle) Abort() (dto.FinalDecision, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.phase != lifecyclePrepared {
		return dto.FinalDecision{}, fmt.Errorf("abort is not legal at height %d", l.height)
	}

	height := l.height
	if err := l.wal.Write(iowal.AbortKey(height), nil); err != nil {
		l.phase = lifecycleFailed
		return dto.FinalDecision{}, fmt.Errorf("write abort decision at height %d: %w", height, err)
	}

	decision := dto.FinalDecision{Height: height, Outcome: dto.OutcomeAbort}
	l.decisions[height] = dto.OutcomeAbort
	l.pendingPayload = nil
	l.phase = lifecycleReady
	l.height++
	return decision, nil
}

func (l *transactionLifecycle) Height() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.height
}

func (l *transactionLifecycle) Decision(height uint64) dto.Outcome {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.decisions[height]
}
