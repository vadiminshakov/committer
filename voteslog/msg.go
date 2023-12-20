package voteslog

import "github.com/vadiminshakov/committer/core/dto"

type msg struct {
	Key   string
	Value []byte
	Idx   uint64
}

func (m msg) Index() uint64 {
	return m.Idx
}

type votesMsg struct {
	Votes []*dto.Vote
	Idx   uint64
}

func (v votesMsg) Index() uint64 {
	return v.Idx
}
