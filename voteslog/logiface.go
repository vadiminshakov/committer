package voteslog

import "github.com/vadiminshakov/committer/core/entity"

type Log interface {
	Set(index uint64, key string, value []byte) error
	Get(index uint64) (string, []byte, bool)
	SetVotes(index uint64, votes []*entity.Vote) error
	GetVotes(index uint64) []*entity.Vote
	Close() error
}
