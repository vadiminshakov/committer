package coordinator

import (
	"errors"
	"fmt"
	"sync"

	"github.com/vadiminshakov/committer/v2/core/dto"
	iowal "github.com/vadiminshakov/committer/v2/io/wal"
)

type lifecycleWAL interface {
	Write(key string, value []byte) error
	Recover(applyFn func(key string, value []byte) error) (*iowal.RecoveryState, error)
}

type lifecyclePhase uint8

const (
	lifecycleReady lifecyclePhase = iota
	lifecyclePrepared
	lifecyclePrecommitted
	lifecycleFailed
)

// transactionLifecycle owns durable state and legal transitions for the one
// transaction a coordinator may have in flight.
type transactionLifecycle struct {
	mu sync.RWMutex

	protocol dto.Protocol
	wal      lifecycleWAL

	height         uint64
	phase          lifecyclePhase
	pendingPayload []byte
	decisions      map[uint64]dto.Outcome
}

// validateUnresolvedRecovery checks that a recovered in-doubt transaction is
// consistent: it must immediately precede the next height, must not also be
// decided, and must carry a decodable payload with a non-empty key.
func validateUnresolvedRecovery(recovery *iowal.RecoveryState) error {
	unresolved := recovery.Unresolved

	if recovery.NextHeight == 0 || unresolved.Height != recovery.NextHeight-1 {
		return fmt.Errorf(
			"unresolved transaction height %d must immediately precede next height %d",
			unresolved.Height,
			recovery.NextHeight,
		)
	}

	if _, decided := recovery.Decisions[unresolved.Height]; decided {
		return fmt.Errorf(
			"transaction at height %d is both unresolved and final",
			unresolved.Height,
		)
	}

	decoded, err := iowal.Decode(unresolved.Payload)
	if err != nil {
		return fmt.Errorf(
			"decode unresolved transaction at height %d: %w",
			unresolved.Height,
			err,
		)
	}

	if decoded.Key == "" {
		return fmt.Errorf(
			"unresolved transaction at height %d has empty transaction key",
			unresolved.Height,
		)
	}

	return nil
}

func newTransactionLifecycle(
	protocol dto.Protocol,
	wal lifecycleWAL,
) (*transactionLifecycle, *dto.FinalDecision, error) {
	if protocol != dto.ProtocolTwoPhase && protocol != dto.ProtocolThreePhase {
		return nil, nil, fmt.Errorf("unsupported transaction protocol %d", protocol)
	}

	if wal == nil {
		return nil, nil, errors.New("transaction WAL is nil")
	}

	recovery, err := wal.Recover(nil)
	if err != nil {
		return nil, nil, fmt.Errorf("recover transaction WAL: %w", err)
	}

	if recovery == nil {
		return nil, nil, errors.New("recover transaction WAL: recovery state is nil")
	}

	if recovery.Unresolved != nil {
		if err := validateUnresolvedRecovery(recovery); err != nil {
			return nil, nil, err
		}
	}

	lifecycle := &transactionLifecycle{
		protocol:  protocol,
		wal:       wal,
		height:    recovery.NextHeight,
		decisions: make(map[uint64]dto.Outcome, len(recovery.Decisions)),
	}

	if err := applyRecoveredDecisions(lifecycle, recovery); err != nil {
		return nil, nil, err
	}

	if recovery.Unresolved == nil {
		return lifecycle, nil, nil
	}

	decision, err := resumeUnresolvedRecovery(lifecycle, recovery, protocol)
	if err != nil {
		return nil, nil, err
	}

	return lifecycle, decision, nil
}

// applyRecoveredDecisions replays durable decisions into the lifecycle.
func applyRecoveredDecisions(lifecycle *transactionLifecycle, recovery *iowal.RecoveryState) error {
	for height, phase := range recovery.Decisions {
		if height >= recovery.NextHeight {
			return fmt.Errorf(
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
			return fmt.Errorf("invalid recovered decision %q at height %d", phase, height)
		}
	}

	return nil
}

// resumeUnresolvedRecovery resumes the in-doubt transaction, returning a
// decision that still needs cohort delivery. Callers must check that
// recovery.Unresolved is non-nil first.
func resumeUnresolvedRecovery(
	lifecycle *transactionLifecycle,
	recovery *iowal.RecoveryState,
	protocol dto.Protocol,
) (*dto.FinalDecision, error) {
	unresolved := recovery.Unresolved
	lifecycle.height = unresolved.Height

	lifecycle.pendingPayload = append([]byte(nil), unresolved.Payload...)
	switch unresolved.Phase {
	case iowal.PhaseKeyPrepared:
		lifecycle.phase = lifecyclePrepared

		decision, err := lifecycle.Abort()
		if err != nil {
			return nil, fmt.Errorf("recover prepared transaction at height %d: %w", unresolved.Height, err)
		}

		return &decision, nil
	case iowal.PhaseKeyPrecommit:
		if protocol != dto.ProtocolThreePhase {
			return nil, fmt.Errorf("precommit recovery at height %d requires three-phase protocol", unresolved.Height)
		}

		decoded, err := iowal.Decode(lifecycle.pendingPayload)
		if err != nil {
			return nil, fmt.Errorf("decode recovered precommit at height %d: %w", unresolved.Height, err)
		}

		if decoded.Key == "" {
			return nil, fmt.Errorf("recovered precommit at height %d has empty transaction key", unresolved.Height)
		}

		lifecycle.phase = lifecyclePrecommitted

		decision, err := lifecycle.Commit()
		if err != nil {
			return nil, fmt.Errorf("recover precommitted transaction at height %d: %w", unresolved.Height, err)
		}

		decision.RequirePrecommit = true

		return &decision, nil
	default:
		return nil, fmt.Errorf("unsupported unresolved phase %q at height %d", unresolved.Phase, unresolved.Height)
	}
}

func (l *transactionLifecycle) Prepare(transaction dto.Transaction) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if transaction.Key == "" {
		return 0, fmt.Errorf("%w: transaction key is empty", ErrInvalidTransaction)
	}

	if l.phase != lifecycleReady {
		return 0, fmt.Errorf("%w: transaction at height %d is not resolved", ErrCoordinatorNotReady, l.height)
	}

	payload, err := iowal.Encode(iowal.Tx{Key: transaction.Key, Value: transaction.Value})
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
