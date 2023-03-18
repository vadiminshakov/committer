package src

import (
	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/core/entity"
)

func Propose(req *entity.ProposeRequest) bool {
	log.Infof("propose hook on height %d is OK", req.Height)
	return true
}

func Commit(req *entity.CommitRequest) bool {
	log.Infof("commit hook on height %d is OK", req.Height)
	return true
}
