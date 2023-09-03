package voteslog

import "github.com/vadiminshakov/committer/core/entity"

type msg struct {
	Index uint64
	Key   string
	Value []byte
}

type votesMsg struct {
	Index uint64
	Votes []*entity.Vote
}
