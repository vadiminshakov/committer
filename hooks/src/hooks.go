package src

import (
	pb "github.com/vadiminshakov/committer/proto"
)

func Propose(req *pb.ProposeRequest) bool {
	return true
}

func Commit(req *pb.CommitRequest) bool {
	return true
}
