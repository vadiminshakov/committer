package cohort

import (
	"github.com/vadiminshakov/committer/core/entity"
)

type PrecommitRequest struct {
	Index uint64         `protobuf:"varint,1,opt,name=index,proto3" json:"index,omitempty"`
	Votes []*entity.Vote `protobuf:"bytes,2,rep,name=votes,proto3" json:"votes,omitempty"`
}

type Mode string

const THREE_PHASE Mode = "three-phase"
