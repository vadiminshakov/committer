package coordinator

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/io/gateway/grpc/server"
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
	atomic.AddUint64(&coord.height, 1)
	require.Equal(t, uint64(1), coord.Height())
}

func TestCoordinator_SyncHeight(t *testing.T) {
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

	// test sync to higher height
	coord.syncHeight(5)
	require.Equal(t, uint64(5), coord.Height())

	// test sync to lower height (should not change)
	coord.syncHeight(3)
	require.Equal(t, uint64(5), coord.Height())

	// test sync to same height (should not change)
	coord.syncHeight(5)
	require.Equal(t, uint64(5), coord.Height())
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

	// expect WAL.Get to return the test data
	mockWAL.EXPECT().Get(uint64(0)).Return(testKey, testValue, nil)

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

	// expect WAL.Get to return no data
	mockWAL.EXPECT().Get(uint64(0)).Return("", nil, nil)

	// test persist message when no data in WAL
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
}

func TestCoordinator_SyncHeight_Concurrent(t *testing.T) {
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

	// test concurrent height sync
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(height uint64) {
			coord.syncHeight(height)
			done <- true
		}(uint64(i + 1))
	}

	// wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// height should be the maximum value
	require.Equal(t, uint64(10), coord.Height())
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
