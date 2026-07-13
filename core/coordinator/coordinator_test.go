package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vadiminshakov/committer/core/dto"
	iowal "github.com/vadiminshakov/committer/io/wal"
	"github.com/vadiminshakov/committer/mocks"
	"go.uber.org/mock/gomock"
)

type coordinatorEventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *coordinatorEventLog) add(event string) {
	l.mu.Lock()
	l.events = append(l.events, event)
	l.mu.Unlock()
}

func (l *coordinatorEventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

func newCoordinatorJournalMock(t *testing.T, events *coordinatorEventLog) *mocks.MockCoordinatorWAL {
	t.Helper()
	journal := mocks.NewMockCoordinatorWAL(gomock.NewController(t))
	journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(0), nil)
	journal.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(key string, _ []byte) error {
		phase, _, ok := iowal.ParseKey(key)
		if !ok {
			return fmt.Errorf("unexpected journal key %q", key)
		}
		events.add("wal:" + phase)
		return nil
	}).AnyTimes()
	return journal
}

func newCoordinatorStoreMock(t *testing.T, events *coordinatorEventLog, putErr error) *mocks.MockCoordinatorStateStore {
	t.Helper()
	store := mocks.NewMockCoordinatorStateStore(gomock.NewController(t))
	store.EXPECT().Put(gomock.Any(), gomock.Any()).DoAndReturn(func(string, []byte) error {
		if events != nil {
			events.add("store:put")
		}
		return putErr
	}).AnyTimes()
	return store
}

func newHealthyCoordinatorPersistence(t *testing.T) (*mocks.MockCoordinatorWAL, *mocks.MockCoordinatorStateStore) {
	t.Helper()
	journal, store := newLifecycleMocks(t)
	journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(0), nil)
	journal.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	store.EXPECT().Put(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return journal, store
}

func TestCoordinatorTwoPhaseReturnsCommittedTransactionHeight(t *testing.T) {
	journal, store := newHealthyCoordinatorPersistence(t)

	coordinator, err := New(dto.ProtocolTwoPhase, journal, store, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	response, err := coordinator.Broadcast(context.Background(), dto.BroadcastRequest{
		Key:   "account",
		Value: []byte("open"),
	})
	require.NoError(t, err)
	require.Equal(t, &dto.BroadcastResponse{
		Type:   dto.ResponseTypeAck,
		Height: 0,
	}, response)
	require.Equal(t, uint64(1), coordinator.Height())
	require.Equal(t, dto.OutcomeCommit, coordinator.Decision(0))
}

func TestCoordinatorThreePhasePersistsAndAppliesBeforeFinalDelivery(t *testing.T) {
	events := &coordinatorEventLog{}
	cohort := mocks.NewMockCoordinatorCohort(gomock.NewController(t))
	cohort.EXPECT().Addr().Return("cohort-a").AnyTimes()
	cohort.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, dto.Proposal) (dto.ParticipantReply, error) {
			events.add("cohort:propose")
			return dto.ParticipantReply{Accepted: true}, nil
		}).Times(1)
	cohort.EXPECT().Precommit(gomock.Any(), uint64(0)).DoAndReturn(
		func(context.Context, uint64) (dto.ParticipantReply, error) {
			events.add("cohort:precommit")
			return dto.ParticipantReply{Accepted: true}, nil
		}).Times(1)
	cohort.EXPECT().ApplyFinalDecision(gomock.Any(), dto.FinalDecision{Height: 0, Outcome: dto.OutcomeCommit}).DoAndReturn(
		func(context.Context, dto.FinalDecision) (dto.ParticipantReply, error) {
			events.add("cohort:decide")
			return dto.ParticipantReply{Accepted: true}, nil
		}).Times(1)
	cohort.EXPECT().Close().Return(nil).Times(1)

	coordinator, err := New(
		dto.ProtocolThreePhase,
		newCoordinatorJournalMock(t, events),
		newCoordinatorStoreMock(t, events, nil),
		[]Cohort{cohort},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	response, err := coordinator.Broadcast(context.Background(), dto.BroadcastRequest{
		Key:   "invoice",
		Value: []byte("paid"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(0), response.Height)
	require.NoError(t, coordinator.Close())
	require.Equal(t, []string{
		"wal:prepared",
		"cohort:propose",
		"wal:precommit",
		"cohort:precommit",
		"wal:commit",
		"store:put",
		"cohort:decide",
	}, events.snapshot())
}

func TestCoordinatorFencesCommittedTransactionWhenLocalApplyFails(t *testing.T) {
	applyErr := errors.New("store unavailable")
	journal, store := newLifecycleMocks(t)
	journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(0), nil)
	journal.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).Times(2)
	store.EXPECT().Put("ledger", []byte("entry")).Return(applyErr)
	cohort := mocks.NewMockCoordinatorCohort(gomock.NewController(t))
	cohort.EXPECT().Addr().Return("cohort-a").AnyTimes()
	cohort.EXPECT().Propose(gomock.Any(), gomock.Any()).Return(dto.ParticipantReply{Accepted: true}, nil)
	cohort.EXPECT().Close().Return(nil)
	cohort.EXPECT().ApplyFinalDecision(gomock.Any(), gomock.Any()).Times(0)

	coordinator, err := New(
		dto.ProtocolTwoPhase,
		journal,
		store,
		[]Cohort{cohort},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	response, err := coordinator.Broadcast(context.Background(), dto.BroadcastRequest{
		Key:   "ledger",
		Value: []byte("entry"),
	})
	require.Equal(t, &dto.BroadcastResponse{Type: dto.ResponseTypeNack, Height: 0}, response)
	var committedNotApplied *CommittedNotAppliedError
	require.ErrorAs(t, err, &committedNotApplied)
	require.ErrorIs(t, err, applyErr)
	require.Equal(t, uint64(0), committedNotApplied.Height)
	require.Equal(t, dto.OutcomeCommit, coordinator.Decision(0))
	require.Equal(t, uint64(0), coordinator.Height())

	_, err = coordinator.Broadcast(context.Background(), dto.BroadcastRequest{
		Key:   "next",
		Value: []byte("blocked"),
	})
	require.ErrorContains(t, err, "not resolved")
}

func TestCoordinatorProposalFailureAbort(t *testing.T) {
	run := func(t *testing.T, failProposal func() (dto.ParticipantReply, error)) {
		t.Helper()

		journal, store := newLifecycleMocks(t)
		gomock.InOrder(
			journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(0), nil),
			journal.EXPECT().Write(iowal.PreparedKey(0), gomock.Any()).Return(nil),
			journal.EXPECT().Write(iowal.AbortKey(0), gomock.Any()).Return(nil),
		)
		cohort := mocks.NewMockCoordinatorCohort(gomock.NewController(t))
		cohort.EXPECT().Addr().Return("cohort-a").AnyTimes()
		cohort.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(
			func(context.Context, dto.Proposal) (dto.ParticipantReply, error) {
				return failProposal()
			}).Times(1)
		cohort.EXPECT().ApplyFinalDecision(gomock.Any(), dto.FinalDecision{
			Height: 0, Outcome: dto.OutcomeAbort,
		}).Return(dto.ParticipantReply{Accepted: true}, nil).Times(1)
		cohort.EXPECT().Close().Return(nil).Times(1)

		coordinator, err := New(
			dto.ProtocolTwoPhase,
			journal,
			store,
			[]Cohort{cohort},
			nil,
		)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

		response, err := coordinator.Broadcast(context.Background(), dto.BroadcastRequest{
			Key:   "order",
			Value: []byte("cancel"),
		})
		require.ErrorContains(t, err, "failed to send propose")
		require.ErrorIs(t, err, ErrProposeVote)
		require.Equal(t, dto.ResponseTypeNack, response.Type)
		require.Equal(t, dto.OutcomeAbort, coordinator.Decision(0))
		require.Equal(t, uint64(1), coordinator.Height())
	}

	t.Run("nack", func(t *testing.T) {
		run(t, func() (dto.ParticipantReply, error) {
			return dto.ParticipantReply{Accepted: false}, nil
		})
	})
	t.Run("transport error", func(t *testing.T) {
		run(t, func() (dto.ParticipantReply, error) {
			return dto.ParticipantReply{}, errors.New("connection lost")
		})
	})
}

func TestCoordinatorPrecommitFailureStaysInDoubt(t *testing.T) {
	journal, store := newLifecycleMocks(t)
	gomock.InOrder(
		journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(0), nil),
		journal.EXPECT().Write(iowal.PreparedKey(0), gomock.Any()).Return(nil),
		journal.EXPECT().Write(iowal.PrecommitKey(0), gomock.Any()).Return(nil),
	)
	cohort := mocks.NewMockCoordinatorCohort(gomock.NewController(t))
	cohort.EXPECT().Addr().Return("cohort-a").AnyTimes()
	cohort.EXPECT().Propose(gomock.Any(), gomock.Any()).Return(dto.ParticipantReply{Accepted: true}, nil)
	cohort.EXPECT().Precommit(gomock.Any(), uint64(0)).DoAndReturn(
		func(context.Context, uint64) (dto.ParticipantReply, error) {
			return dto.ParticipantReply{}, errors.New("precommit unavailable")
		})
	cohort.EXPECT().ApplyFinalDecision(gomock.Any(), gomock.Any()).Times(0)
	cohort.EXPECT().Close().Return(nil)

	coordinator, err := New(
		dto.ProtocolThreePhase,
		journal,
		store,
		[]Cohort{cohort},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	response, err := coordinator.Broadcast(context.Background(), dto.BroadcastRequest{
		Key:   "invoice",
		Value: []byte("pending"),
	})
	require.ErrorContains(t, err, "failed to send precommit")
	require.ErrorIs(t, err, ErrPrecommitVote)
	require.Equal(t, dto.ResponseTypeNack, response.Type)
	require.Equal(t, dto.OutcomeUnknown, coordinator.Decision(0))
	require.Equal(t, uint64(0), coordinator.Height())

	_, err = coordinator.Broadcast(context.Background(), dto.BroadcastRequest{Key: "next"})
	require.ErrorContains(t, err, "not resolved")
}

func TestCoordinatorAbortJournalError(t *testing.T) {
	abortErr := errors.New("abort journal unavailable")
	journal, store := newLifecycleMocks(t)
	gomock.InOrder(
		journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(0), nil),
		journal.EXPECT().Write(iowal.PreparedKey(0), gomock.Any()).Return(nil),
		journal.EXPECT().Write(iowal.AbortKey(0), gomock.Any()).Return(abortErr),
	)
	cohort := mocks.NewMockCoordinatorCohort(gomock.NewController(t))
	cohort.EXPECT().Addr().Return("cohort-a").AnyTimes()
	cohort.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, dto.Proposal) (dto.ParticipantReply, error) {
			return dto.ParticipantReply{Accepted: false}, nil
		})
	cohort.EXPECT().Close().Return(nil)

	coordinator, err := New(
		dto.ProtocolTwoPhase,
		journal,
		store,
		[]Cohort{cohort},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	response, err := coordinator.Broadcast(context.Background(), dto.BroadcastRequest{
		Key:   "order",
		Value: []byte("cancel"),
	})
	require.Equal(t, &dto.BroadcastResponse{Type: dto.ResponseTypeNack, Height: 0}, response)
	require.ErrorIs(t, err, abortErr)
	require.ErrorContains(t, err, "failed to send propose")
	require.NotErrorIs(t, err, ErrProposeVote)
}

func TestCoordinatorFinalDeliveryDoesNotDelayCommittedResponse(t *testing.T) {
	finalEntered := make(chan struct{})
	releaseFinal := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseFinal) }) })
	cohort := mocks.NewMockCoordinatorCohort(gomock.NewController(t))
	cohort.EXPECT().Addr().Return("cohort-a").AnyTimes()
	cohort.EXPECT().Propose(gomock.Any(), gomock.Any()).
		Return(dto.ParticipantReply{Accepted: true}, nil).Times(1)
	cohort.EXPECT().ApplyFinalDecision(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, _ dto.FinalDecision) (dto.ParticipantReply, error) {
		close(finalEntered)
		select {
		case <-releaseFinal:
			return dto.ParticipantReply{Accepted: true}, nil
		case <-ctx.Done():
			return dto.ParticipantReply{}, ctx.Err()
		}
	}).Times(1)
	cohort.EXPECT().Close().Return(nil).Times(1)
	journal, store := newHealthyCoordinatorPersistence(t)

	coordinator, err := New(
		dto.ProtocolTwoPhase,
		journal,
		store,
		[]Cohort{cohort},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	type broadcastResult struct {
		response *dto.BroadcastResponse
		err      error
	}
	result := make(chan broadcastResult, 1)
	go func() {
		response, err := coordinator.Broadcast(context.Background(), dto.BroadcastRequest{
			Key:   "key",
			Value: []byte("value"),
		})
		result <- broadcastResult{response: response, err: err}
	}()

	select {
	case <-finalEntered:
	case <-time.After(time.Second):
		require.FailNow(t, "final decision delivery did not start")
	}
	select {
	case committed := <-result:
		require.NoError(t, committed.err)
		require.Equal(t, &dto.BroadcastResponse{Type: dto.ResponseTypeAck, Height: 0}, committed.response)
	case <-time.After(time.Second):
		require.FailNow(t, "client response waited for final decision acknowledgement")
	}

	releaseOnce.Do(func() { close(releaseFinal) })
}

func TestCoordinatorCloseCancelsAndWaitsForInFlightBroadcast(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		proposalEntered := false
		proposalExited := false

		cohort := mocks.NewMockCoordinatorCohort(gomock.NewController(t))
		cohort.EXPECT().Addr().Return("cohort-a").AnyTimes()
		cohort.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, _ dto.Proposal) (dto.ParticipantReply, error) {
				proposalEntered = true
				// keep Broadcast in flight until Coordinator.Close cancels delivery.
				<-ctx.Done()
				proposalExited = true
				return dto.ParticipantReply{}, ctx.Err()
			})
		cohort.EXPECT().ApplyFinalDecision(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, _ dto.FinalDecision) (dto.ParticipantReply, error) {
				<-ctx.Done()
				return dto.ParticipantReply{}, ctx.Err()
			}).AnyTimes()
		cohort.EXPECT().Close().DoAndReturn(func() error {
			// cohorts must remain open until the active vote exits.
			if !proposalExited {
				return errors.New("cohort closed before in-flight proposal exited")
			}
			return nil
		})
		journal, store := newHealthyCoordinatorPersistence(t)

		coordinator, err := New(
			dto.ProtocolTwoPhase,
			journal,
			store,
			[]Cohort{cohort},
			nil,
		)
		require.NoError(t, err)
		defer func() { require.NoError(t, coordinator.Close()) }()

		var broadcastErr error
		go func() {
			_, broadcastErr = coordinator.Broadcast(context.Background(), dto.BroadcastRequest{
				Key:   "key",
				Value: []byte("value"),
			})
		}()

		// wait until Broadcast is blocked inside the cohort's Propose call.
		synctest.Wait()
		require.True(t, proposalEntered)
		require.False(t, proposalExited)

		var closeErr error
		// Close must cancel Propose, wait for Broadcast to release the coordinator
		// lock, and only then close the cohort.
		go func() {
			closeErr = coordinator.Close()
		}()

		// both Close and the canceled Broadcast must complete before assertions.
		synctest.Wait()
		require.NoError(t, closeErr)
		require.Error(t, broadcastErr)
		require.True(t, proposalExited)
		require.Equal(t, dto.OutcomeAbort, coordinator.Decision(0))
	})
}

func TestCoordinatorConstructionFailsClosedWhenRecoveryApplyFails(t *testing.T) {
	applyErr := errors.New("recovery store unavailable")
	cohort := mocks.NewMockCoordinatorCohort(gomock.NewController(t))
	cohort.EXPECT().Addr().Return("cohort-a").AnyTimes()
	cohort.EXPECT().Close().Return(nil).Times(1)
	recovery := &iowal.RecoveryState{
		NextHeight: 1,
		Unresolved: &iowal.UnresolvedTransaction{
			Height:  0,
			Phase:   iowal.PhaseKeyPrecommit,
			Payload: lifecyclePayload(t, "key", "value"),
		},
		Decisions: make(map[uint64]string),
	}
	journal, store := newLifecycleMocks(t)
	gomock.InOrder(
		journal.EXPECT().Recover(gomock.Any()).Return(recovery, nil),
		journal.EXPECT().Write(iowal.CommitKey(0), gomock.Any()).Return(nil),
		store.EXPECT().Put("key", []byte("value")).Return(applyErr),
	)

	coordinator, err := New(
		dto.ProtocolThreePhase,
		journal,
		store,
		[]Cohort{cohort},
		nil,
	)
	require.Nil(t, coordinator)
	var committedNotApplied *CommittedNotAppliedError
	require.ErrorAs(t, err, &committedNotApplied)
	require.ErrorIs(t, err, applyErr)
}

func TestCoordinatorRecoverySendsPrecommitBeforeCommit(t *testing.T) {
	recovery := &iowal.RecoveryState{
		NextHeight: 1,
		Unresolved: &iowal.UnresolvedTransaction{
			Height:  0,
			Phase:   iowal.PhaseKeyPrecommit,
			Payload: lifecyclePayload(t, "recovered", "value"),
		},
		Decisions: make(map[uint64]string),
	}
	journal, store := newLifecycleMocks(t)
	gomock.InOrder(
		journal.EXPECT().Recover(gomock.Any()).Return(recovery, nil),
		journal.EXPECT().Write(iowal.CommitKey(0), gomock.Any()).Return(nil),
		store.EXPECT().Put("recovered", []byte("value")).Return(nil),
	)

	cohort := mocks.NewMockCoordinatorCohort(gomock.NewController(t))
	cohort.EXPECT().Addr().Return("cohort-a").AnyTimes()
	gomock.InOrder(
		cohort.EXPECT().Precommit(gomock.Any(), uint64(0)).
			Return(dto.ParticipantReply{Accepted: true}, nil).Times(1),
		cohort.EXPECT().ApplyFinalDecision(gomock.Any(), dto.FinalDecision{
			Height: 0, Outcome: dto.OutcomeCommit, RequirePrecommit: true,
		}).Return(dto.ParticipantReply{Accepted: true}, nil).Times(1),
		cohort.EXPECT().Close().Return(nil).Times(1),
	)

	coordinator, err := New(
		dto.ProtocolThreePhase,
		journal,
		store,
		[]Cohort{cohort},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })
	require.NoError(t, coordinator.Close())
}
