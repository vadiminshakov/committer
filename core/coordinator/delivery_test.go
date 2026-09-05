package coordinator

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/mocks"
	"go.uber.org/mock/gomock"
)

func TestCohortDeliveryFansOutProposalBeforeWaitingForVotes(t *testing.T) {
	t.Parallel()

	entered := make(chan string, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	cohort := func(name string) Cohort {
		cohort := mocks.NewMockCoordinatorCohort(gomock.NewController(t))
		cohort.EXPECT().Addr().Return(name).AnyTimes()
		cohort.EXPECT().Propose(gomock.Any(), dto.Proposal{Height: 7}).DoAndReturn(func(ctx context.Context, _ dto.Proposal) (dto.ParticipantReply, error) {
			entered <- name
			select {
			case <-release:
				return dto.ParticipantReply{Accepted: true}, nil
			case <-ctx.Done():
				return dto.ParticipantReply{}, ctx.Err()
			}
		})
		cohort.EXPECT().Close().Return(nil)
		return cohort
	}

	delivery := newCohortDelivery([]Cohort{
		cohort("alpha"),
		cohort("beta"),
	}, nil, nil)
	t.Cleanup(func() { require.NoError(t, delivery.Close()) })

	result := make(chan error, 1)
	go func() {
		result <- delivery.VoteProposal(context.Background(), dto.Proposal{Height: 7})
	}()

	seen := map[string]bool{}
	for range 2 {
		select {
		case name := <-entered:
			seen[name] = true
		case <-time.After(time.Second):
			require.FailNow(t, "proposal was not fanned out to every cohort")
		}
	}
	require.Equal(t, map[string]bool{"alpha": true, "beta": true}, seen)

	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.FailNow(t, "proposal vote did not complete")
	}
}

func TestCohortDeliveryFansOutPrecommitAndNormalizesNack(t *testing.T) {
	t.Parallel()

	entered := make(chan string, 2)
	heights := make(chan uint64, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	cohort := func(name string, accepted bool) Cohort {
		cohort := mocks.NewMockCoordinatorCohort(gomock.NewController(t))
		cohort.EXPECT().Addr().Return(name).AnyTimes()
		cohort.EXPECT().Precommit(gomock.Any(), uint64(11)).DoAndReturn(func(ctx context.Context, height uint64) (dto.ParticipantReply, error) {
			heights <- height
			entered <- name
			select {
			case <-release:
				return dto.ParticipantReply{Accepted: accepted, Height: height}, nil
			case <-ctx.Done():
				return dto.ParticipantReply{}, ctx.Err()
			}
		})
		cohort.EXPECT().Close().Return(nil)
		return cohort
	}

	delivery := newCohortDelivery([]Cohort{
		cohort("alpha", true),
		cohort("beta", false),
	}, nil, nil)
	t.Cleanup(func() { require.NoError(t, delivery.Close()) })

	result := make(chan error, 1)
	go func() { result <- delivery.VotePrecommit(context.Background(), 11) }()

	seen := map[string]bool{}
	for range 2 {
		select {
		case name := <-entered:
			seen[name] = true
		case <-time.After(time.Second):
			require.FailNow(t, "precommit was not fanned out to every cohort")
		}
	}
	require.Equal(t, map[string]bool{"alpha": true, "beta": true}, seen)
	require.ElementsMatch(t, []uint64{11, 11}, []uint64{<-heights, <-heights})

	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-result:
		require.ErrorContains(t, err, "beta")
	case <-time.After(time.Second):
		require.FailNow(t, "precommit vote did not complete")
	}
}

func TestCohortDeliveryReturnsAfterStartingFinalRetry(t *testing.T) {
	t.Parallel()

	firstAttempt := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseFirst) }) })
	attempts := make(chan int, 4)
	observedDecisions := make(chan dto.FinalDecision, 4)
	var attemptCount atomic.Int32

	cohort := mocks.NewMockCoordinatorCohort(gomock.NewController(t))
	cohort.EXPECT().Addr().Return("alpha").AnyTimes()
	cohort.EXPECT().ApplyFinalDecision(gomock.Any(), dto.FinalDecision{Height: 13, Outcome: dto.OutcomeCommit}).DoAndReturn(func(ctx context.Context, decision dto.FinalDecision) (dto.ParticipantReply, error) {
		observedDecisions <- decision
		attempt := int(attemptCount.Add(1))
		attempts <- attempt
		switch attempt {
		case 1:
			close(firstAttempt)
			select {
			case <-releaseFirst:
				return dto.ParticipantReply{Accepted: false, Height: 13}, nil
			case <-ctx.Done():
				return dto.ParticipantReply{}, ctx.Err()
			}
		case 2:
			return dto.ParticipantReply{}, fmt.Errorf("temporary transport failure")
		default:
			return dto.ParticipantReply{Accepted: true, Height: 14}, nil
		}
	}).Times(3)
	cohort.EXPECT().Close().Return(nil)

	delivery := newCohortDelivery([]Cohort{cohort}, nil, nil)
	t.Cleanup(func() { require.NoError(t, delivery.Close()) })

	returned := make(chan error, 1)
	go func() {
		returned <- delivery.DeliverFinal(dto.FinalDecision{Height: 13, Outcome: dto.OutcomeCommit})
	}()

	select {
	case <-firstAttempt:
	case <-time.After(time.Second):
		require.FailNow(t, "final decision delivery did not start")
	}
	select {
	case err := <-returned:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.FailNow(t, "DeliverFinal waited for cohort acknowledgement")
	}

	releaseOnce.Do(func() { close(releaseFirst) })
	for expected := 1; expected <= 3; expected++ {
		select {
		case actual := <-attempts:
			require.Equal(t, expected, actual)
			require.Equal(t, dto.FinalDecision{Height: 13, Outcome: dto.OutcomeCommit}, <-observedDecisions)
		case <-time.After(time.Second):
			require.FailNow(t, fmt.Sprintf("final decision attempt %d was not observed", expected))
		}
	}
}

func TestCohortDeliveryDoesNotBlockProposalBehindFinalDecisionRetry(t *testing.T) {
	t.Parallel()

	finalEntered := make(chan struct{})
	releaseFinal := make(chan struct{})
	proposalEntered := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseFinal) }) })

	var callsMu sync.Mutex
	calls := make([]string, 0, 2)
	cohort := mocks.NewMockCoordinatorCohort(gomock.NewController(t))
	cohort.EXPECT().Addr().Return("alpha").AnyTimes()
	cohort.EXPECT().ApplyFinalDecision(gomock.Any(), dto.FinalDecision{Height: 4, Outcome: dto.OutcomeCommit}).DoAndReturn(
		func(ctx context.Context, _ dto.FinalDecision) (dto.ParticipantReply, error) {
			callsMu.Lock()
			calls = append(calls, "decide")
			callsMu.Unlock()
			close(finalEntered)
			select {
			case <-releaseFinal:
				return dto.ParticipantReply{Accepted: true}, nil
			case <-ctx.Done():
				return dto.ParticipantReply{}, ctx.Err()
			}
		})
	cohort.EXPECT().Propose(gomock.Any(), dto.Proposal{Height: 5}).DoAndReturn(
		func(context.Context, dto.Proposal) (dto.ParticipantReply, error) {
			callsMu.Lock()
			calls = append(calls, "propose")
			callsMu.Unlock()
			close(proposalEntered)
			return dto.ParticipantReply{Accepted: true}, nil
		})
	cohort.EXPECT().Close().Return(nil)

	delivery := newCohortDelivery([]Cohort{cohort}, nil, nil)
	t.Cleanup(func() { require.NoError(t, delivery.Close()) })
	require.NoError(t, delivery.DeliverFinal(dto.FinalDecision{Height: 4, Outcome: dto.OutcomeCommit}))

	select {
	case <-finalEntered:
	case <-time.After(time.Second):
		require.FailNow(t, "final decision delivery did not start")
	}

	proposalResult := make(chan error, 1)
	go func() {
		proposalResult <- delivery.VoteProposal(context.Background(), dto.Proposal{Height: 5})
	}()
	select {
	case <-proposalEntered:
	case <-time.After(time.Second):
		require.FailNow(t, "proposal was blocked behind final decision delivery")
	}
	select {
	case err := <-proposalResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.FailNow(t, "proposal vote did not complete")
	}

	releaseOnce.Do(func() { close(releaseFinal) })

	callsMu.Lock()
	require.Equal(t, []string{"decide", "propose"}, calls)
	callsMu.Unlock()
}

func TestCohortDeliveryCatchesUpLaggingParticipantBeforeRetryingProposal(t *testing.T) {
	t.Parallel()

	var callsMu sync.Mutex
	calls := make([]string, 0, 4)
	decisions := make([]dto.FinalDecision, 0, 2)
	var proposals atomic.Int32
	cohort := mocks.NewMockCoordinatorCohort(gomock.NewController(t))
	cohort.EXPECT().Addr().Return("alpha").AnyTimes()
	cohort.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, proposal dto.Proposal) (dto.ParticipantReply, error) {
			callsMu.Lock()
			calls = append(calls, "propose")
			callsMu.Unlock()
			if proposals.Add(1) == 1 {
				return dto.ParticipantReply{Accepted: false, Height: 0}, nil
			}
			return dto.ParticipantReply{Accepted: true, Height: proposal.Height}, nil
		}).Times(2)
	cohort.EXPECT().Precommit(gomock.Any(), uint64(0)).DoAndReturn(
		func(context.Context, uint64) (dto.ParticipantReply, error) {
			callsMu.Lock()
			calls = append(calls, "precommit")
			callsMu.Unlock()
			return dto.ParticipantReply{Accepted: true}, nil
		})
	cohort.EXPECT().ApplyFinalDecision(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, decision dto.FinalDecision) (dto.ParticipantReply, error) {
			callsMu.Lock()
			calls = append(calls, "decide")
			decisions = append(decisions, decision)
			callsMu.Unlock()
			return dto.ParticipantReply{Accepted: true, Height: decision.Height + 1}, nil
		}).Times(2)
	cohort.EXPECT().Close().Return(nil)

	lookup := func(height uint64) dto.Outcome {
		switch height {
		case 0:
			return dto.OutcomeCommit
		case 1:
			return dto.OutcomeAbort
		default:
			return dto.OutcomeUnknown
		}
	}
	delivery := newCohortDelivery([]Cohort{cohort}, lookup, nil)
	t.Cleanup(func() { require.NoError(t, delivery.Close()) })

	err := delivery.VoteProposal(context.Background(), dto.Proposal{
		Height:      2,
		Protocol:    dto.ProtocolThreePhase,
		Transaction: dto.Transaction{Key: "key", Value: []byte("value")},
	})
	require.NoError(t, err)

	callsMu.Lock()
	require.Equal(t, []string{"propose", "precommit", "decide", "decide", "propose"}, calls)
	require.Equal(t, []dto.FinalDecision{
		{Height: 0, Outcome: dto.OutcomeCommit, RequirePrecommit: true},
		{Height: 1, Outcome: dto.OutcomeAbort},
	}, decisions)
	callsMu.Unlock()
}

func TestCohortDeliveryContinuesCatchUpWhenLaggingParticipantAdvances(t *testing.T) {
	t.Parallel()

	var proposals atomic.Int32
	var decisionsMu sync.Mutex
	decisionHeights := make([]uint64, 0, 8)
	cohort := mocks.NewMockCoordinatorCohort(gomock.NewController(t))
	cohort.EXPECT().Addr().Return("alpha").AnyTimes()
	cohort.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, proposal dto.Proposal) (dto.ParticipantReply, error) {
			switch proposals.Add(1) {
			case 1:
				return dto.ParticipantReply{Accepted: false, Height: 0}, nil
			case 2:
				return dto.ParticipantReply{Accepted: false, Height: 2}, nil
			default:
				return dto.ParticipantReply{Accepted: true, Height: proposal.Height}, nil
			}
		}).Times(3)
	cohort.EXPECT().ApplyFinalDecision(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, decision dto.FinalDecision) (dto.ParticipantReply, error) {
			decisionsMu.Lock()
			decisionHeights = append(decisionHeights, decision.Height)
			decisionsMu.Unlock()
			return dto.ParticipantReply{Accepted: true, Height: decision.Height + 1}, nil
		}).Times(8)
	cohort.EXPECT().Close().Return(nil)

	lookup := func(height uint64) dto.Outcome {
		if height >= 5 {
			return dto.OutcomeUnknown
		}
		return dto.OutcomeAbort
	}
	delivery := newCohortDelivery([]Cohort{cohort}, lookup, nil)
	t.Cleanup(func() { require.NoError(t, delivery.Close()) })

	require.NoError(t, delivery.VoteProposal(context.Background(), dto.Proposal{Height: 5}))
	require.Equal(t, int32(3), proposals.Load())
	decisionsMu.Lock()
	require.Equal(t, []uint64{0, 1, 2, 3, 4, 2, 3, 4}, decisionHeights)
	decisionsMu.Unlock()
}

func TestCohortDeliveryStopsCatchUpWhenParticipantDoesNotAdvance(t *testing.T) {
	t.Parallel()

	var proposals atomic.Int32
	var decisions atomic.Int32
	cohort := mocks.NewMockCoordinatorCohort(gomock.NewController(t))
	cohort.EXPECT().Addr().Return("alpha").AnyTimes()
	cohort.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, dto.Proposal) (dto.ParticipantReply, error) {
			proposals.Add(1)
			return dto.ParticipantReply{Accepted: false, Height: 0}, nil
		}).Times(2)
	cohort.EXPECT().ApplyFinalDecision(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, decision dto.FinalDecision) (dto.ParticipantReply, error) {
			decisions.Add(1)
			return dto.ParticipantReply{Accepted: true, Height: decision.Height + 1}, nil
		}).Times(2)
	cohort.EXPECT().Close().Return(nil)
	lookup := func(height uint64) dto.Outcome {
		if height >= 2 {
			return dto.OutcomeUnknown
		}
		return dto.OutcomeAbort
	}
	delivery := newCohortDelivery([]Cohort{cohort}, lookup, nil)
	t.Cleanup(func() { require.NoError(t, delivery.Close()) })

	err := delivery.VoteProposal(context.Background(), dto.Proposal{Height: 2})
	require.ErrorContains(t, err, "did not advance")
	require.Equal(t, int32(2), proposals.Load())
	require.Equal(t, int32(2), decisions.Load(), "the same decisions must not be replayed forever")
}

func TestCohortDeliveryCloseUnblocksCallersAndClosesAdaptersOnce(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	exited := make(chan struct{})
	var closes atomic.Int32
	cohort := mocks.NewMockCoordinatorCohort(gomock.NewController(t))
	cohort.EXPECT().Addr().Return("alpha").AnyTimes()
	cohort.EXPECT().Propose(gomock.Any(), dto.Proposal{Height: 1}).DoAndReturn(
		func(ctx context.Context, _ dto.Proposal) (dto.ParticipantReply, error) {
			close(entered)
			<-ctx.Done()
			close(exited)
			return dto.ParticipantReply{}, ctx.Err()
		})
	cohort.EXPECT().Close().DoAndReturn(func() error {
		select {
		case <-exited:
		default:
			return fmt.Errorf("adapter closed while a delivery task was still using it")
		}
		closes.Add(1)
		return nil
	})

	delivery := newCohortDelivery([]Cohort{cohort}, nil, nil)
	result := make(chan error, 1)
	go func() {
		result <- delivery.VoteProposal(context.Background(), dto.Proposal{Height: 1})
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		require.FailNow(t, "proposal delivery did not start")
	}
	require.NoError(t, delivery.Close())
	select {
	case err := <-result:
		require.Error(t, err)
	case <-time.After(time.Second):
		require.FailNow(t, "Close did not unblock the proposal caller")
	}
	require.Equal(t, int32(1), closes.Load())
	require.NoError(t, delivery.Close())
	require.Equal(t, int32(1), closes.Load())
}
