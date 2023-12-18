package hooks

import (
	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/core/dto"
)

func Propose(req *dto.ProposeRequest) bool {
	log.Infof("propose hook on height %d is OK", req.Height)
	return true
}

func Commit(req *dto.CommitRequest) bool {
	log.Infof("commit hook on height %d is OK", req.Height)
	return true
}
