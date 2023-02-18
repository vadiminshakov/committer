package server

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/vadiminshakov/committer/algoplagin"
	"github.com/vadiminshakov/committer/cache"
	"github.com/vadiminshakov/committer/db"
	"github.com/vadiminshakov/committer/entity"
	pb "github.com/vadiminshakov/committer/proto"
	"os"
	"testing"
)

const BADGER = "/tmp/testbadger"

func TestMain(m *testing.M) {
	os.Mkdir(BADGER, os.FileMode(0777))

	m.Run()

	os.RemoveAll(BADGER)
}

func TestProposeHandler(t *testing.T) {
	var propose = func(req *entity.ProposeRequest) bool {
		return true
	}
	c := cache.New()
	s := &Server{NodeCache: c, committer: algoplagin.NewCommitter(nil, c, propose, nil)}

	req := &pb.ProposeRequest{Key: "testkey", Value: []byte("testvalue"), CommitType: pb.CommitType_THREE_PHASE_COMMIT}
	response, err := s.ProposeHandler(context.Background(), req)
	assert.NoError(t, err, "ProposeHandler returned not nil error")
	assert.Equal(t, response.Type, pb.Type_ACK, "response should contain ACK")
}

func TestCommitHandler(t *testing.T) {
	var commit = func(req *entity.CommitRequest) bool {
		return true
	}
	NodeCache := cache.New()
	NodeCache.Set(1, "testkey", []byte("testvalue"))
	database, err := db.New(BADGER)
	assert.NoError(t, err, "failed to create test database")
	req := &pb.CommitRequest{Index: 1}

	s := &Server{NodeCache: NodeCache, DB: database, committer: algoplagin.NewCommitter(database, NodeCache, nil, commit)}
	response, err := s.CommitHandler(context.Background(), req)
	assert.NoError(t, err, "CommitHandler returned not nil error")
	assert.Equal(t, response.Type, pb.Type_ACK, "response should contain ACK")
}
