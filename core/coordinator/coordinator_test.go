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
	"github.com/vadiminshakov/committer/v2/core/dto"
	iowal "github.com/vadiminshakov/committer/v2/io/wal"
	"github.com/vadiminshakov/committer/v2/mocks"
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

func newHealthyCoordinatorWAL(t *testing.T) *mocks.MockCoordinatorWAL {
	t.Helper()
	journal := newLifecycleWAL(t)
	journal.EXPECT().Recover(gomock.Any()).Return(cleanRecovery(0), nil)
	journal.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	return journal
}

func TestCoordinatorTwoPhaseReturnsCommittedTransactionHeight(t *testing.T) {
	journal := newHealthyCoordinatorWAL(t)

	coordinator, err := newCoordinator(dto.ProtocolTwoPhase, journal, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	height, err := coordinator.Commit(context.Background(), "account", []byte("open"))
	require.NoError(t, err)
	require.Equal(t, uint64(0), height)
	require.Equal(t, uint64(1), coordinator.Height())
	require.Equal(t, dto.OutcomeCommit, coordinator.Decision(0))
}

func TestCoordinatorThreePhasePersistsBeforeFinalDelivery(t *testing.T) {
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

	coordinator, err := newCoordinator(
		dto.ProtocolThreePhase,
		newCoordinatorJournalMock(t, events),
		[]Cohort{cohort},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	height, err := coordinator.Commit(context.Background(), "invoice", []byte("paid"))
	require.NoError(t, err)
	require.Equal(t, uint64(0), height)
	require.NoError(t, coordinator.Close())
	require.Equal(t, []string{
		"wal:prepared",
		"cohort:propose",
		"wal:precommit",
		"cohort:precommit",
		"wal:commit",
		"cohort:decide",
	}, events.snapshot())
}

func TestCoordinatorProposalFailureAbort(t *testing.T) {
	run := func(t *testing.T, failProposal func() (dto.ParticipantReply, error)) {
		t.Helper()

		journal := newLifecycleWAL(t)
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

		coordinator, err := newCoordinator(
			dto.ProtocolTwoPhase,
			journal,
			[]Cohort{cohort},
			nil,
		)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

		height, err := coordinator.Commit(context.Background(), "order", []byte("cancel"))
		require.ErrorContains(t, err, "transaction aborted")
		require.ErrorIs(t, err, dto.ErrAborted)
		require.Equal(t, uint64(0), height)
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
	journal := newLifecycleWAL(t)
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

	coordinator, err := newCoordinator(
		dto.ProtocolThreePhase,
		journal,
		[]Cohort{cohort},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	height, err := coordinator.Commit(context.Background(), "invoice", []byte("pending"))
	require.ErrorContains(t, err, "failed to send precommit")
	require.ErrorIs(t, err, dto.ErrPrecommitVote)
	require.Equal(t, uint64(0), height)
	require.Equal(t, dto.OutcomeUnknown, coordinator.Decision(0))
	require.Equal(t, uint64(0), coordinator.Height())

	_, err = coordinator.Commit(context.Background(), "next", nil)
	require.ErrorContains(t, err, "not resolved")
}

func TestCoordinatorAbortJournalError(t *testing.T) {
	abortErr := errors.New("abort journal unavailable")
	journal := newLifecycleWAL(t)
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

	coordinator, err := newCoordinator(
		dto.ProtocolTwoPhase,
		journal,
		[]Cohort{cohort},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	height, err := coordinator.Commit(context.Background(), "order", []byte("cancel"))
	require.Equal(t, uint64(0), height)
	require.ErrorIs(t, err, abortErr)
	require.ErrorContains(t, err, "failed to send propose")
	require.NotErrorIs(t, err, dto.ErrAborted)
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

	journal := newHealthyCoordinatorWAL(t)

	coordinator, err := newCoordinator(
		dto.ProtocolTwoPhase,
		journal,
		[]Cohort{cohort},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	type broadcastResult struct {
		height uint64
		err    error
	}

	result := make(chan broadcastResult, 1)

	go func() {
		height, err := coordinator.Commit(context.Background(), "key", []byte("value"))
		result <- broadcastResult{height: height, err: err}
	}()

	select {
	case <-finalEntered:
	case <-time.After(time.Second):
		require.FailNow(t, "final decision delivery did not start")
	}

	select {
	case committed := <-result:
		require.NoError(t, committed.err)
		require.Equal(t, uint64(0), committed.height)
	case <-time.After(time.Second):
		require.FailNow(t, "client response waited for final decision acknowledgement")
	}

	releaseOnce.Do(func() { close(releaseFinal) })
}

func TestCoordinatorCloseCancelsAndWaitsForInFlightCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		proposalEntered := false
		proposalExited := false

		cohort := mocks.NewMockCoordinatorCohort(gomock.NewController(t))
		cohort.EXPECT().Addr().Return("cohort-a").AnyTimes()
		cohort.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, _ dto.Proposal) (dto.ParticipantReply, error) {
				proposalEntered = true
				// keep Commit in flight until Coordinator.Close cancels delivery.
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

		journal := newHealthyCoordinatorWAL(t)

		coordinator, err := newCoordinator(
			dto.ProtocolTwoPhase,
			journal,
			[]Cohort{cohort},
			nil,
		)

		require.NoError(t, err)
		defer func() { require.NoError(t, coordinator.Close()) }()

		var broadcastErr error
		go func() {
			_, broadcastErr = coordinator.Commit(context.Background(), "key", []byte("value"))
		}()

		// wait until Commit is blocked inside the cohort's Propose call.
		synctest.Wait()
		require.True(t, proposalEntered)
		require.False(t, proposalExited)

		var closeErr error
		// Close must cancel Propose, wait for Commit to release the coordinator
		// lock, and only then close the cohort.
		go func() {
			closeErr = coordinator.Close()
		}()

		// both Close and the canceled Commit must complete before assertions.
		synctest.Wait()
		require.NoError(t, closeErr)
		require.Error(t, broadcastErr)
		require.True(t, proposalExited)
		require.Equal(t, dto.OutcomeAbort, coordinator.Decision(0))
	})
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
	journal := newLifecycleWAL(t)
	gomock.InOrder(
		journal.EXPECT().Recover(gomock.Any()).Return(recovery, nil),
		journal.EXPECT().Write(iowal.CommitKey(0), gomock.Any()).Return(nil),
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

	coordinator, err := newCoordinator(
		dto.ProtocolThreePhase,
		journal,
		[]Cohort{cohort},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })
	require.NoError(t, coordinator.Close())
}
