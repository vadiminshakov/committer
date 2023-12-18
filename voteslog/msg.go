package voteslog

import "github.com/vadiminshakov/committer/core/dto"

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
	Votes []*dto.Vote
}

func (v votesMsg) Index() uint64 {
	return v.Idx
}
