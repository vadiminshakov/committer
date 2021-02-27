package src

import (
	log "github.com/sirupsen/logrus"
	pb "github.com/vadiminshakov/committer/proto"
)

func Propose(req *pb.ProposeRequest) bool {
	log.Infof("propose hook on height %d is OK", req.Index)
	return true
}

func Commit(req *pb.CommitRequest) bool {
	log.Infof("commit hook on height %d is OK", req.Index)
	return true
}
