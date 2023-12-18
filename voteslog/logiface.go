package voteslog

import "github.com/vadiminshakov/committer/core/dto"

type Log interface {
	Set(index uint64, key string, value []byte) error
	Get(index uint64) (string, []byte, bool)
	SetVotes(index uint64, votes []*dto.Vote) error
	GetVotes(index uint64) []*dto.Vote
	Close() error
}
