package coordinator

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vadiminshakov/committer/core/dto"
	iowal "github.com/vadiminshakov/committer/io/wal"
	"github.com/vadiminshakov/committer/mocks"
	"go.uber.org/mock/gomock"
)

func newLifecycleMocks(t *testing.T) (*mocks.MockCoordinatorWAL, *mocks.MockCoordinatorStateStore) {
	t.Helper()
	ctrl := gomock.NewController(t)
	return mocks.NewMockCoordinatorWAL(ctrl), mocks.NewMockCoordinatorStateStore(ctrl)
}

func cleanRecovery(height uint64) *iowal.RecoveryState {
	return &iowal.RecoveryState{NextHeight: height, Decisions: make(map[uint64]string)}
}

func lifecyclePayload(t *testing.T, key, value string) []byte {
	t.Helper()
	payload, err := iowal.Encode(iowal.Tx{Key: key, Value: []byte(value)})
	require.NoError(t, err)
	return payload
}

func TestTransactionLifecycleOwnsRecoveryAndAppliesResolvedCommits(t *testing.T) {
	journal, store := newLifecycleMocks(t)
	recovery := &iowal.RecoveryState{
		NextHeight: 1,
		Decisions:  map[uint64]string{0: iowal.PhaseKeyCommit},
	}
	journal.EXPECT().Recover(gomock.Any()).DoAndReturn(
		func(apply func(string, []byte) error) (*iowal.RecoveryState, error) {
			require.NoError(t, apply("recovered", []byte("value")))
			return recovery, nil
		},
	)
	store.EXPECT().Put("recovered", []byte("value")).Return(nil)

	lifecycle, recovered, err := newTransactionLifecycle(dto.ProtocolTwoPhase, journal, store)
	require.NoError(t, err)
	require.Nil(t, recovered)
	require.Equal(t, dto.OutcomeCommit, lifecycle.Decision(0))
	require.Equal(t, uint64(1), lifecycle.Height())
}

func TestTransactionLifecycleTwoPhaseCommit(t *testing.T) {
	journal, store := newLifecycleMocks(t)
	gomock.InOrder(
		journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(7), nil),
		journal.EXPECT().Write(iowal.PreparedKey(7), gomock.Any()).Return(nil),
		journal.EXPECT().Write(iowal.CommitKey(7), gomock.Any()).Return(nil),
		store.EXPECT().Put("account", []byte("open")).Return(nil),
	)

	lifecycle, recovered, err := newTransactionLifecycle(dto.ProtocolTwoPhase, journal, store)
	require.NoError(t, err)
	require.Nil(t, recovered)

	height, err := lifecycle.Prepare(dto.Transaction{Key: "account", Value: []byte("open")})
	require.NoError(t, err)
	require.Equal(t, uint64(7), height)
	require.Equal(t, uint64(7), lifecycle.Height())
	require.Equal(t, dto.OutcomeUnknown, lifecycle.Decision(7))

	decision, err := lifecycle.Commit()
	require.NoError(t, err)
	require.Equal(t, dto.FinalDecision{Height: 7, Outcome: dto.OutcomeCommit}, decision)
	require.Equal(t, uint64(8), lifecycle.Height())
	require.Equal(t, dto.OutcomeCommit, lifecycle.Decision(7))
}

func TestTransactionLifecycleThreePhaseCommit(t *testing.T) {
	journal, store := newLifecycleMocks(t)
	gomock.InOrder(
		journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(3), nil),
		journal.EXPECT().Write(iowal.PreparedKey(3), gomock.Any()).Return(nil),
		journal.EXPECT().Write(iowal.PrecommitKey(3), gomock.Any()).Return(nil),
		journal.EXPECT().Write(iowal.CommitKey(3), gomock.Any()).Return(nil),
		store.EXPECT().Put("invoice", []byte("paid")).Return(nil),
	)

	lifecycle, recovered, err := newTransactionLifecycle(dto.ProtocolThreePhase, journal, store)
	require.NoError(t, err)
	require.Nil(t, recovered)

	height, err := lifecycle.Prepare(dto.Transaction{Key: "invoice", Value: []byte("paid")})
	require.NoError(t, err)
	require.Equal(t, uint64(3), height)
	require.NoError(t, lifecycle.Precommit())
	decision, err := lifecycle.Commit()
	require.NoError(t, err)
	require.Equal(t, dto.FinalDecision{Height: 3, Outcome: dto.OutcomeCommit}, decision)
	require.Equal(t, uint64(4), lifecycle.Height())
}

func TestTransactionLifecycleAbortPublishesOnlyDurableDecision(t *testing.T) {
	journal, store := newLifecycleMocks(t)
	gomock.InOrder(
		journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(11), nil),
		journal.EXPECT().Write(iowal.PreparedKey(11), gomock.Any()).Return(nil),
		journal.EXPECT().Write(iowal.AbortKey(11), gomock.Any()).Return(nil),
	)

	lifecycle, _, err := newTransactionLifecycle(dto.ProtocolThreePhase, journal, store)
	require.NoError(t, err)
	_, err = lifecycle.Prepare(dto.Transaction{Key: "order", Value: []byte("cancelled")})
	require.NoError(t, err)
	decision, err := lifecycle.Abort()
	require.NoError(t, err)
	require.Equal(t, dto.FinalDecision{Height: 11, Outcome: dto.OutcomeAbort}, decision)
	require.Equal(t, dto.OutcomeAbort, lifecycle.Decision(11))
	require.Equal(t, uint64(12), lifecycle.Height())
}

func TestTransactionLifecycleFencesAfterDurableCommitCannotBeApplied(t *testing.T) {
	applyErr := errors.New("store unavailable")
	journal, store := newLifecycleMocks(t)
	gomock.InOrder(
		journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(5), nil),
		journal.EXPECT().Write(iowal.PreparedKey(5), gomock.Any()).Return(nil),
		journal.EXPECT().Write(iowal.CommitKey(5), gomock.Any()).Return(nil),
		store.EXPECT().Put("ledger", []byte("entry")).Return(applyErr),
	)

	lifecycle, _, err := newTransactionLifecycle(dto.ProtocolTwoPhase, journal, store)
	require.NoError(t, err)
	_, err = lifecycle.Prepare(dto.Transaction{Key: "ledger", Value: []byte("entry")})
	require.NoError(t, err)
	decision, err := lifecycle.Commit()
	require.Equal(t, dto.FinalDecision{Height: 5, Outcome: dto.OutcomeCommit}, decision)
	var committedNotApplied *CommittedNotAppliedError
	require.ErrorAs(t, err, &committedNotApplied)
	require.Equal(t, uint64(5), committedNotApplied.Height)
	require.ErrorIs(t, err, applyErr)
	require.Equal(t, dto.OutcomeCommit, lifecycle.Decision(5))
	require.Equal(t, uint64(5), lifecycle.Height())
	_, err = lifecycle.Prepare(dto.Transaction{Key: "next", Value: []byte("blocked")})
	require.ErrorIs(t, err, ErrCoordinatorNotReady)
}

func TestTransactionLifecycleRejectsEmptyKeyBeforePrepareIsDurable(t *testing.T) {
	journal, store := newLifecycleMocks(t)
	journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(0), nil)
	lifecycle, _, err := newTransactionLifecycle(dto.ProtocolTwoPhase, journal, store)
	require.NoError(t, err)
	_, err = lifecycle.Prepare(dto.Transaction{Value: []byte("invalid")})
	require.ErrorIs(t, err, ErrInvalidTransaction)
	require.Equal(t, uint64(0), lifecycle.Height())
	require.Equal(t, dto.OutcomeUnknown, lifecycle.Decision(0))
}

func TestTransactionLifecycleFailsClosedWhenPreparedWriteReportsError(t *testing.T) {
	writeErr := errors.New("uncertain journal write")
	journal, store := newLifecycleMocks(t)
	journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(2), nil)
	journal.EXPECT().Write(iowal.PreparedKey(2), gomock.Any()).Return(writeErr)
	lifecycle, _, err := newTransactionLifecycle(dto.ProtocolTwoPhase, journal, store)
	require.NoError(t, err)
	_, err = lifecycle.Prepare(dto.Transaction{Key: "first", Value: []byte("value")})
	require.ErrorIs(t, err, writeErr)
	_, err = lifecycle.Prepare(dto.Transaction{Key: "second", Value: []byte("must not start")})
	require.ErrorIs(t, err, ErrCoordinatorNotReady)
	require.Equal(t, uint64(2), lifecycle.Height())
}

func TestTransactionLifecycleConstructionFailsWhenJournalRecoveryFails(t *testing.T) {
	recoverErr := errors.New("journal replay unavailable")
	journal, store := newLifecycleMocks(t)
	journal.EXPECT().Recover(gomock.Any()).Return(nil, recoverErr)
	lifecycle, recovered, err := newTransactionLifecycle(dto.ProtocolTwoPhase, journal, store)
	require.ErrorIs(t, err, recoverErr)
	require.ErrorContains(t, err, "recover transaction WAL")
	require.Nil(t, lifecycle)
	require.Nil(t, recovered)
}

func TestTransactionLifecycleConstructionFailsWhenJournalReplayCannotApplyCommit(t *testing.T) {
	applyErr := errors.New("replay store unavailable")
	journal, store := newLifecycleMocks(t)
	journal.EXPECT().Recover(gomock.Any()).DoAndReturn(
		func(apply func(string, []byte) error) (*iowal.RecoveryState, error) {
			if err := apply("recovered", []byte("value")); err != nil {
				return nil, err
			}
			return cleanRecovery(1), nil
		},
	)
	store.EXPECT().Put("recovered", []byte("value")).Return(applyErr)
	lifecycle, recovered, err := newTransactionLifecycle(dto.ProtocolTwoPhase, journal, store)
	require.ErrorIs(t, err, applyErr)
	require.ErrorContains(t, err, "recover transaction WAL")
	require.Nil(t, lifecycle)
	require.Nil(t, recovered)
}

func TestTransactionLifecycleFailsClosedWhenPrecommitWriteReportsError(t *testing.T) {
	writeErr := errors.New("uncertain journal write")
	journal, store := newLifecycleMocks(t)
	gomock.InOrder(
		journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(4), nil),
		journal.EXPECT().Write(iowal.PreparedKey(4), gomock.Any()).Return(nil),
		journal.EXPECT().Write(iowal.PrecommitKey(4), gomock.Any()).Return(writeErr),
	)
	lifecycle, _, err := newTransactionLifecycle(dto.ProtocolThreePhase, journal, store)
	require.NoError(t, err)
	_, err = lifecycle.Prepare(dto.Transaction{Key: "key", Value: []byte("value")})
	require.NoError(t, err)
	require.ErrorIs(t, lifecycle.Precommit(), writeErr)
	require.Error(t, lifecycle.Precommit())
	_, err = lifecycle.Abort()
	require.Error(t, err)
	require.Equal(t, dto.OutcomeUnknown, lifecycle.Decision(4))
}

func TestTransactionLifecycleFailsClosedWhenCommitWriteReportsError(t *testing.T) {
	writeErr := errors.New("uncertain journal write")
	journal, store := newLifecycleMocks(t)
	gomock.InOrder(
		journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(6), nil),
		journal.EXPECT().Write(iowal.PreparedKey(6), gomock.Any()).Return(nil),
		journal.EXPECT().Write(iowal.CommitKey(6), gomock.Any()).Return(writeErr),
	)
	lifecycle, _, err := newTransactionLifecycle(dto.ProtocolTwoPhase, journal, store)
	require.NoError(t, err)
	_, err = lifecycle.Prepare(dto.Transaction{Key: "key", Value: []byte("value")})
	require.NoError(t, err)
	decision, err := lifecycle.Commit()
	require.ErrorIs(t, err, writeErr)
	require.Equal(t, dto.FinalDecision{}, decision)
	_, err = lifecycle.Commit()
	require.Error(t, err)
	require.Equal(t, dto.OutcomeUnknown, lifecycle.Decision(6))
	require.Equal(t, uint64(6), lifecycle.Height())
}

func TestTransactionLifecycleFailsClosedWhenAbortWriteReportsError(t *testing.T) {
	writeErr := errors.New("uncertain journal write")
	journal, store := newLifecycleMocks(t)
	gomock.InOrder(
		journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(8), nil),
		journal.EXPECT().Write(iowal.PreparedKey(8), gomock.Any()).Return(nil),
		journal.EXPECT().Write(iowal.AbortKey(8), gomock.Any()).Return(writeErr),
	)
	lifecycle, _, err := newTransactionLifecycle(dto.ProtocolTwoPhase, journal, store)
	require.NoError(t, err)
	_, err = lifecycle.Prepare(dto.Transaction{Key: "key", Value: []byte("value")})
	require.NoError(t, err)
	decision, err := lifecycle.Abort()
	require.ErrorIs(t, err, writeErr)
	require.Equal(t, dto.FinalDecision{}, decision)
	_, err = lifecycle.Abort()
	require.Error(t, err)
	require.Equal(t, dto.OutcomeUnknown, lifecycle.Decision(8))
	require.Equal(t, uint64(8), lifecycle.Height())
}

func TestTransactionLifecycleRecoversPreparedAsDurableAbort(t *testing.T) {
	journal, store := newLifecycleMocks(t)
	recovery := &iowal.RecoveryState{
		NextHeight: 10,
		Unresolved: &iowal.UnresolvedTransaction{Height: 9, Phase: iowal.PhaseKeyPrepared, Payload: lifecyclePayload(t, "pending", "value")},
		Decisions:  map[uint64]string{7: iowal.PhaseKeyCommit, 8: iowal.PhaseKeyAbort},
	}
	gomock.InOrder(
		journal.EXPECT().Recover(gomock.Any()).Return(recovery, nil),
		journal.EXPECT().Write(iowal.AbortKey(9), gomock.Any()).Return(nil),
	)
	lifecycle, recovered, err := newTransactionLifecycle(dto.ProtocolTwoPhase, journal, store)
	require.NoError(t, err)
	require.Equal(t, &dto.FinalDecision{Height: 9, Outcome: dto.OutcomeAbort}, recovered)
	require.Equal(t, dto.OutcomeCommit, lifecycle.Decision(7))
	require.Equal(t, dto.OutcomeAbort, lifecycle.Decision(8))
	require.Equal(t, dto.OutcomeAbort, lifecycle.Decision(9))
	require.Equal(t, uint64(10), lifecycle.Height())
}

func TestTransactionLifecycleConstructionFailsWhenRecoveredAbortIsNotDurable(t *testing.T) {
	writeErr := errors.New("journal unavailable")
	journal, store := newLifecycleMocks(t)
	recovery := &iowal.RecoveryState{
		NextHeight: 2,
		Unresolved: &iowal.UnresolvedTransaction{Height: 1, Phase: iowal.PhaseKeyPrepared, Payload: lifecyclePayload(t, "pending", "value")},
		Decisions:  make(map[uint64]string),
	}
	gomock.InOrder(
		journal.EXPECT().Recover(gomock.Any()).Return(recovery, nil),
		journal.EXPECT().Write(iowal.AbortKey(1), gomock.Any()).Return(writeErr),
	)
	lifecycle, recovered, err := newTransactionLifecycle(dto.ProtocolTwoPhase, journal, store)
	require.ErrorIs(t, err, writeErr)
	require.Nil(t, lifecycle)
	require.Nil(t, recovered)
}

func TestTransactionLifecycleRejectsFinalDecisionConflictingWithUnresolvedHeight(t *testing.T) {
	journal, store := newLifecycleMocks(t)
	recovery := &iowal.RecoveryState{
		NextHeight: 1,
		Unresolved: &iowal.UnresolvedTransaction{Height: 0, Phase: iowal.PhaseKeyPrepared, Payload: lifecyclePayload(t, "pending", "value")},
		Decisions:  map[uint64]string{0: iowal.PhaseKeyCommit},
	}
	journal.EXPECT().Recover(gomock.Any()).Return(recovery, nil)
	lifecycle, recovered, err := newTransactionLifecycle(dto.ProtocolTwoPhase, journal, store)
	require.ErrorContains(t, err, "both unresolved and final")
	require.Nil(t, lifecycle)
	require.Nil(t, recovered)
}

func TestTransactionLifecycleRejectsStaleUnresolvedHeight(t *testing.T) {
	journal, store := newLifecycleMocks(t)
	recovery := &iowal.RecoveryState{
		NextHeight: 5,
		Unresolved: &iowal.UnresolvedTransaction{Height: 2, Phase: iowal.PhaseKeyPrepared, Payload: lifecyclePayload(t, "stale", "value")},
		Decisions:  map[uint64]string{3: iowal.PhaseKeyAbort, 4: iowal.PhaseKeyCommit},
	}
	journal.EXPECT().Recover(gomock.Any()).Return(recovery, nil)
	lifecycle, recovered, err := newTransactionLifecycle(dto.ProtocolTwoPhase, journal, store)
	require.ErrorContains(t, err, "must immediately precede next height")
	require.Nil(t, lifecycle)
	require.Nil(t, recovered)
}

func TestTransactionLifecycleRejectsDecisionAtOrAboveNextHeight(t *testing.T) {
	journal, store := newLifecycleMocks(t)
	recovery := &iowal.RecoveryState{NextHeight: 4, Decisions: map[uint64]string{4: iowal.PhaseKeyCommit}}
	journal.EXPECT().Recover(gomock.Any()).Return(recovery, nil)
	lifecycle, recovered, err := newTransactionLifecycle(dto.ProtocolTwoPhase, journal, store)
	require.ErrorContains(t, err, "is not below next height")
	require.Nil(t, lifecycle)
	require.Nil(t, recovered)
}

func TestTransactionLifecycleRecoversThreePhasePrecommitAsAppliedCommit(t *testing.T) {
	journal, store := newLifecycleMocks(t)
	recovery := &iowal.RecoveryState{
		NextHeight: 13,
		Unresolved: &iowal.UnresolvedTransaction{Height: 12, Phase: iowal.PhaseKeyPrecommit, Payload: lifecyclePayload(t, "invoice", "paid")},
		Decisions:  make(map[uint64]string),
	}
	gomock.InOrder(
		journal.EXPECT().Recover(gomock.Any()).Return(recovery, nil),
		journal.EXPECT().Write(iowal.CommitKey(12), gomock.Any()).Return(nil),
		store.EXPECT().Put("invoice", []byte("paid")).Return(nil),
	)
	lifecycle, recovered, err := newTransactionLifecycle(dto.ProtocolThreePhase, journal, store)
	require.NoError(t, err)
	require.Equal(t, &dto.FinalDecision{Height: 12, Outcome: dto.OutcomeCommit, RequirePrecommit: true}, recovered)
	require.Equal(t, dto.OutcomeCommit, lifecycle.Decision(12))
	require.Equal(t, uint64(13), lifecycle.Height())
}

func TestTransactionLifecycleRejectsCorruptPrecommitBeforeRecoveredCommit(t *testing.T) {
	journal, store := newLifecycleMocks(t)
	recovery := &iowal.RecoveryState{
		NextHeight: 1,
		Unresolved: &iowal.UnresolvedTransaction{Height: 0, Phase: iowal.PhaseKeyPrecommit, Payload: []byte("corrupt")},
		Decisions:  make(map[uint64]string),
	}
	journal.EXPECT().Recover(gomock.Any()).Return(recovery, nil)
	lifecycle, recovered, err := newTransactionLifecycle(dto.ProtocolThreePhase, journal, store)
	require.Error(t, err)
	require.Nil(t, lifecycle)
	require.Nil(t, recovered)
}

func TestTransactionLifecycleRejectsCorruptPreparedPayloadBeforeRecoveredAbort(t *testing.T) {
	journal, store := newLifecycleMocks(t)
	recovery := &iowal.RecoveryState{
		NextHeight: 1,
		Unresolved: &iowal.UnresolvedTransaction{Height: 0, Phase: iowal.PhaseKeyPrepared, Payload: []byte("corrupt")},
		Decisions:  make(map[uint64]string),
	}
	journal.EXPECT().Recover(gomock.Any()).Return(recovery, nil)
	lifecycle, recovered, err := newTransactionLifecycle(dto.ProtocolTwoPhase, journal, store)
	require.ErrorContains(t, err, "decode unresolved transaction")
	require.Nil(t, lifecycle)
	require.Nil(t, recovered)
}

func TestTransactionLifecycleRejectsRecoveredPrecommitForTwoPhaseProtocol(t *testing.T) {
	journal, store := newLifecycleMocks(t)
	recovery := &iowal.RecoveryState{
		NextHeight: 1,
		Unresolved: &iowal.UnresolvedTransaction{Height: 0, Phase: iowal.PhaseKeyPrecommit, Payload: lifecyclePayload(t, "pending", "value")},
		Decisions:  make(map[uint64]string),
	}
	journal.EXPECT().Recover(gomock.Any()).Return(recovery, nil)
	lifecycle, recovered, err := newTransactionLifecycle(dto.ProtocolTwoPhase, journal, store)
	require.ErrorContains(t, err, "requires three-phase protocol")
	require.Nil(t, lifecycle)
	require.Nil(t, recovered)
}

func TestTransactionLifecycleConstructionFailsWhenRecoveredCommitCannotBeApplied(t *testing.T) {
	applyErr := errors.New("store unavailable")
	journal, store := newLifecycleMocks(t)
	recovery := &iowal.RecoveryState{
		NextHeight: 4,
		Unresolved: &iowal.UnresolvedTransaction{Height: 3, Phase: iowal.PhaseKeyPrecommit, Payload: lifecyclePayload(t, "pending", "value")},
		Decisions:  make(map[uint64]string),
	}
	gomock.InOrder(
		journal.EXPECT().Recover(gomock.Any()).Return(recovery, nil),
		journal.EXPECT().Write(iowal.CommitKey(3), gomock.Any()).Return(nil),
		store.EXPECT().Put("pending", []byte("value")).Return(applyErr),
	)
	lifecycle, recovered, err := newTransactionLifecycle(dto.ProtocolThreePhase, journal, store)
	var committedNotApplied *CommittedNotAppliedError
	require.ErrorAs(t, err, &committedNotApplied)
	require.ErrorIs(t, err, applyErr)
	require.Equal(t, uint64(3), committedNotApplied.Height)
	require.Nil(t, lifecycle)
	require.Nil(t, recovered)
}

func TestTransactionLifecycleRestartImportsResolvedFactsWithoutRepeatingSideEffects(t *testing.T) {
	journal, store := newLifecycleMocks(t)
	recovery := &iowal.RecoveryState{
		NextHeight: 2,
		Decisions: map[uint64]string{
			0: iowal.PhaseKeyCommit,
			1: iowal.PhaseKeyAbort,
		},
	}
	journal.EXPECT().Recover(gomock.Any()).Return(recovery, nil)
	lifecycle, recovered, err := newTransactionLifecycle(dto.ProtocolThreePhase, journal, store)
	require.NoError(t, err)
	require.Nil(t, recovered)
	require.Equal(t, dto.OutcomeCommit, lifecycle.Decision(0))
	require.Equal(t, dto.OutcomeAbort, lifecycle.Decision(1))
	require.Equal(t, uint64(2), lifecycle.Height())
}

func TestTransactionLifecycleUsesRecoveredFinalFactsIdempotently(t *testing.T) {
	journal, store := newLifecycleMocks(t)
	recovery := &iowal.RecoveryState{
		NextHeight: 2,
		Decisions: map[uint64]string{
			0: iowal.PhaseKeyCommit,
			1: iowal.PhaseKeyAbort,
		},
	}
	journal.EXPECT().Recover(gomock.Any()).Return(recovery, nil)
	lifecycle, recovered, err := newTransactionLifecycle(dto.ProtocolTwoPhase, journal, store)
	require.NoError(t, err)
	require.Nil(t, recovered)
	require.Equal(t, dto.OutcomeCommit, lifecycle.Decision(0))
	require.Equal(t, dto.OutcomeAbort, lifecycle.Decision(1))
	require.Equal(t, uint64(2), lifecycle.Height())
}

func TestTransactionLifecycleEnforcesProtocolTransitions(t *testing.T) {
	t.Run("two-phase has no precommit", func(t *testing.T) {
		journal, store := newLifecycleMocks(t)
		gomock.InOrder(
			journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(0), nil),
			journal.EXPECT().Write(iowal.PreparedKey(0), gomock.Any()).Return(nil),
			journal.EXPECT().Write(iowal.CommitKey(0), gomock.Any()).Return(nil),
			store.EXPECT().Put("key", []byte("value")).Return(nil),
		)
		lifecycle, _, err := newTransactionLifecycle(dto.ProtocolTwoPhase, journal, store)
		require.NoError(t, err)
		_, err = lifecycle.Prepare(dto.Transaction{Key: "key", Value: []byte("value")})
		require.NoError(t, err)
		require.Error(t, lifecycle.Precommit())
		_, err = lifecycle.Commit()
		require.NoError(t, err)
	})

	t.Run("three-phase cannot commit before precommit", func(t *testing.T) {
		journal, store := newLifecycleMocks(t)
		gomock.InOrder(
			journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(0), nil),
			journal.EXPECT().Write(iowal.PreparedKey(0), gomock.Any()).Return(nil),
			journal.EXPECT().Write(iowal.AbortKey(0), gomock.Any()).Return(nil),
		)
		lifecycle, _, err := newTransactionLifecycle(dto.ProtocolThreePhase, journal, store)
		require.NoError(t, err)
		_, err = lifecycle.Prepare(dto.Transaction{Key: "key", Value: []byte("value")})
		require.NoError(t, err)
		_, err = lifecycle.Commit()
		require.Error(t, err)
		_, err = lifecycle.Abort()
		require.NoError(t, err)
	})

	t.Run("precommit is no longer abortable", func(t *testing.T) {
		journal, store := newLifecycleMocks(t)
		gomock.InOrder(
			journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(0), nil),
			journal.EXPECT().Write(iowal.PreparedKey(0), gomock.Any()).Return(nil),
			journal.EXPECT().Write(iowal.PrecommitKey(0), gomock.Any()).Return(nil),
			journal.EXPECT().Write(iowal.CommitKey(0), gomock.Any()).Return(nil),
			store.EXPECT().Put("key", []byte("value")).Return(nil),
		)
		lifecycle, _, err := newTransactionLifecycle(dto.ProtocolThreePhase, journal, store)
		require.NoError(t, err)
		_, err = lifecycle.Prepare(dto.Transaction{Key: "key", Value: []byte("value")})
		require.NoError(t, err)
		require.NoError(t, lifecycle.Precommit())
		_, err = lifecycle.Abort()
		require.Error(t, err)
		_, err = lifecycle.Commit()
		require.NoError(t, err)
	})
}
