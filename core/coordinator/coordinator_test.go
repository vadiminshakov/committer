package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
	iowal "github.com/vadiminshakov/committer/io/wal"
	"github.com/vadiminshakov/committer/mocks"
	"go.uber.org/mock/gomock"
)

func TestCoordinator_New(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWAL := mocks.NewMockwal(ctrl)
	mockStore := mocks.NewMockStateStore(ctrl)

	conf := &config.Config{
		Nodeaddr:   "localhost:8080",
		Role:       "coordinator",
		Cohorts:    []string{"localhost:8081", "localhost:8082"},
		CommitType: server.TWO_PHASE,
	}

	coord, err := New(conf, mockWAL, mockStore)
	require.NoError(t, err)
	require.NotNil(t, coord)
	require.Equal(t, uint64(0), coord.Height())
	require.Len(t, coord.cohorts, 2)
}

func TestCoordinator_Height(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWAL := mocks.NewMockwal(ctrl)
	mockStore := mocks.NewMockStateStore(ctrl)

	conf := &config.Config{
		Nodeaddr:   "localhost:8080",
		Role:       "coordinator",
		Cohorts:    []string{},
		CommitType: server.TWO_PHASE,
	}

	coord, err := New(conf, mockWAL, mockStore)
	require.NoError(t, err)

	// test initial height
	require.Equal(t, uint64(0), coord.Height())

	// test height increment
	coord.height.Add(1)
	require.Equal(t, uint64(1), coord.Height())
}

func TestCoordinator_PersistMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWAL := mocks.NewMockwal(ctrl)
	mockStore := mocks.NewMockStateStore(ctrl)

	conf := &config.Config{
		Nodeaddr:   "localhost:8080",
		Role:       "coordinator",
		Cohorts:    []string{},
		CommitType: server.TWO_PHASE,
	}

	coord, err := New(conf, mockWAL, mockStore)
	require.NoError(t, err)

	// test persist message success
	testKey := "test-key"
	testValue := []byte("test-value")

	ptx := iowal.Tx{Key: testKey, Value: testValue}
	encoded, _ := iowal.Encode(ptx)

	// set pending payload as if propose() had already written it
	coord.pendingPayload = encoded

	// expect DB.Put to be called with the test data
	mockStore.EXPECT().Put(testKey, testValue).Return(nil)

	err = coord.persistMessage()
	require.NoError(t, err)
}

func TestCoordinator_PersistMessage_NoDataInWAL(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWAL := mocks.NewMockwal(ctrl)
	mockStore := mocks.NewMockStateStore(ctrl)

	conf := &config.Config{
		Nodeaddr:   "localhost:8080",
		Role:       "coordinator",
		Cohorts:    []string{},
		CommitType: server.TWO_PHASE,
	}

	coord, err := New(conf, mockWAL, mockStore)
	require.NoError(t, err)

	// walPreparedIdx is 0 (default), so persistMessage returns "can't find msg" immediately
	err = coord.persistMessage()
	require.Error(t, err)
	require.Contains(t, err.Error(), "can't find msg in wal")
}

func TestCoordinator_Abort(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWAL := mocks.NewMockwal(ctrl)
	mockStore := mocks.NewMockStateStore(ctrl)

	conf := &config.Config{
		Nodeaddr:   "localhost:8080",
		Role:       "coordinator",
		Cohorts:    []string{},
		CommitType: server.TWO_PHASE,
	}

	coord, err := New(conf, mockWAL, mockStore)
	require.NoError(t, err)

	ctx := context.Background()

	mockWAL.EXPECT().Write(iowal.AbortKey(0), []byte(nil)).Return(nil)

	// test abort
	done := make(chan bool, 1)
	go func() {
		coord.abort(ctx, "test abort reason")
		done <- true
	}()

	select {
	case <-done:
		// abort completed successfully
	case <-time.After(1 * time.Second):
		t.Fatal("abort took too long to complete")
	}

	// an aborted height is consumed and its decision is recorded
	require.Equal(t, uint64(1), coord.Height())
	require.Equal(t, dto.OutcomeAbort, coord.Decision(0))
}

func TestCoordinator_CommitRecordsDecisionBeforeDelivery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWAL := mocks.NewMockwal(ctrl)
	mockStore := mocks.NewMockStateStore(ctrl)

	conf := &config.Config{
		Nodeaddr:   "localhost:8080",
		Role:       "coordinator",
		Cohorts:    []string{},
		CommitType: server.TWO_PHASE,
	}

	coord, err := New(conf, mockWAL, mockStore)
	require.NoError(t, err)

	payload, err := iowal.Encode(iowal.Tx{Key: "k", Value: []byte("v")})
	require.NoError(t, err)
	coord.pendingPayload = payload

	// the commit record is force-written and the tx applied locally
	mockWAL.EXPECT().Write(iowal.CommitKey(0), payload).Return(nil)
	mockStore.EXPECT().Put("k", []byte("v")).Return(nil)

	require.NoError(t, coord.commit(context.Background()))

	// the decision is queryable by in-doubt cohorts
	require.Equal(t, dto.OutcomeCommit, coord.Decision(0))
	require.Nil(t, coord.pendingPayload)
}

func TestCoordinator_CommitWALFailureResolvesAsAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWAL := mocks.NewMockwal(ctrl)
	mockStore := mocks.NewMockStateStore(ctrl)

	conf := &config.Config{
		Nodeaddr:   "localhost:8080",
		Role:       "coordinator",
		Cohorts:    []string{},
		CommitType: server.TWO_PHASE,
	}

	coord, err := New(conf, mockWAL, mockStore)
	require.NoError(t, err)

	payload, err := iowal.Encode(iowal.Tx{Key: "k", Value: []byte("v")})
	require.NoError(t, err)
	coord.pendingPayload = payload

	// commit record cannot be written: no decision record on disk means abort
	mockWAL.EXPECT().Write(iowal.CommitKey(0), payload).Return(fmt.Errorf("disk full"))
	mockWAL.EXPECT().Write(iowal.AbortKey(0), []byte(nil)).Return(fmt.Errorf("disk full"))

	require.Error(t, coord.commit(context.Background()))

	require.Equal(t, dto.OutcomeAbort, coord.Decision(0))
	require.Equal(t, uint64(1), coord.Height())
}

func TestCoordinator_Recover_PresumedAbort(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWAL := mocks.NewMockwal(ctrl)
	mockStore := mocks.NewMockStateStore(ctrl)

	conf := &config.Config{
		Nodeaddr:   "localhost:8080",
		Role:       "coordinator",
		Cohorts:    []string{},
		CommitType: server.TWO_PHASE,
	}

	coord, err := New(conf, mockWAL, mockStore)
	require.NoError(t, err)

	// the node crashed at height 5 after writing prepared but before the decision:
	// recovery must resolve it as abort (presumed abort)
	mockWAL.EXPECT().Write(iowal.AbortKey(5), []byte(nil)).Return(nil)

	coord.Recover(&iowal.RecoveryState{
		NextHeight: 6,
		Unresolved: &iowal.UnresolvedTransaction{
			Height:  5,
			Phase:   iowal.PhaseKeyPrepared,
			Payload: []byte("pending"),
		},
		Decisions: map[uint64]string{
			3: iowal.PhaseKeyCommit,
			4: iowal.PhaseKeyAbort,
		},
	})

	require.Equal(t, uint64(6), coord.Height())
	require.Equal(t, dto.OutcomeAbort, coord.Decision(5))

	// recovered decisions answer termination protocol requests
	require.Equal(t, dto.OutcomeCommit, coord.Decision(3))
	require.Equal(t, dto.OutcomeAbort, coord.Decision(4))
	require.Equal(t, dto.OutcomeUnknown, coord.Decision(6))
}

func TestCoordinator_Recover_PresumedCommit_3PC(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWAL := mocks.NewMockwal(ctrl)
	mockStore := mocks.NewMockStateStore(ctrl)

	conf := &config.Config{
		Nodeaddr:   "localhost:8080",
		Role:       "coordinator",
		Cohorts:    []string{},
		CommitType: server.THREE_PHASE,
	}

	coord, err := New(conf, mockWAL, mockStore)
	require.NoError(t, err)

	payload, err := iowal.Encode(iowal.Tx{Key: "k", Value: []byte("v")})
	require.NoError(t, err)

	// the node crashed at height 5 after every cohort acked precommit: a
	// cohort past that point autocommits on its own timeout even without the
	// coordinator, so recovery must resolve it as commit (presumed commit),
	// not abort
	mockWAL.EXPECT().Write(iowal.CommitKey(5), payload).Return(nil)
	mockStore.EXPECT().Put("k", []byte("v")).Return(nil)

	coord.Recover(&iowal.RecoveryState{
		NextHeight: 6,
		Unresolved: &iowal.UnresolvedTransaction{
			Height:  5,
			Phase:   iowal.PhaseKeyPrecommit,
			Payload: payload,
		},
		Decisions: map[uint64]string{},
	})

	require.Equal(t, uint64(6), coord.Height())
	require.Equal(t, dto.OutcomeCommit, coord.Decision(5))
}

func TestCoordinator_Recover_CleanState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWAL := mocks.NewMockwal(ctrl)
	mockStore := mocks.NewMockStateStore(ctrl)

	conf := &config.Config{
		Nodeaddr:   "localhost:8080",
		Role:       "coordinator",
		Cohorts:    []string{},
		CommitType: server.TWO_PHASE,
	}

	coord, err := New(conf, mockWAL, mockStore)
	require.NoError(t, err)

	// no pending transaction: nothing to resolve, no WAL writes expected
	coord.Recover(&iowal.RecoveryState{
		NextHeight: 2,
		Decisions:  map[uint64]string{0: iowal.PhaseKeyCommit, 1: iowal.PhaseKeyCommit},
	})

	require.Equal(t, uint64(2), coord.Height())
	require.Equal(t, dto.OutcomeCommit, coord.Decision(0))
	require.Equal(t, dto.OutcomeUnknown, coord.Decision(2))
}

func TestNackResponse(t *testing.T) {
	testErr := fmt.Errorf("test error")
	testMsg := "test message"

	resp, err := nackResponse(testErr, testMsg)

	require.NotNil(t, resp)
	require.Equal(t, dto.ResponseTypeNack, resp.Type)
	require.Error(t, err)
	require.Contains(t, err.Error(), testMsg)
	require.Contains(t, err.Error(), "test error")
}

func TestCoordinator_Config_Validation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWAL := mocks.NewMockwal(ctrl)
	mockStore := mocks.NewMockStateStore(ctrl)

	// test with empty cohorts list
	conf := &config.Config{
		Nodeaddr:   "localhost:8080",
		Role:       "coordinator",
		Cohorts:    []string{},
		CommitType: server.TWO_PHASE,
	}

	coord, err := New(conf, mockWAL, mockStore)
	require.NoError(t, err)
	require.NotNil(t, coord)
	require.Len(t, coord.cohorts, 0)

	// test with multiple cohorts
	conf.Cohorts = []string{"localhost:8081", "localhost:8082", "localhost:8083"}
	coord, err = New(conf, mockWAL, mockStore)
	require.NoError(t, err)
	require.NotNil(t, coord)
	require.Len(t, coord.cohorts, 3)
}
