package voteslog

import "github.com/vadiminshakov/committer/core/entity"

type msg struct {
	Idx   uint64
	Key   string
	Value []byte
}

func (m msg) Index() uint64 {
	return m.Idx
}

type votesMsg struct {
	Idx   uint64
	Votes []*entity.Vote
}

func (v votesMsg) Index() uint64 {
	return v.Idx
}
