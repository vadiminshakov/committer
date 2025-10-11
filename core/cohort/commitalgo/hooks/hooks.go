// Package hooks provides an extensible hook system for commit algorithms.
//
// Hooks allow custom validation, metrics collection, and business logic
// to be executed during propose and commit phases without modifying core logic.
package hooks

import (
	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/core/dto"
)

// DefaultHook provides the default logging behavior
type DefaultHook struct{}

// NewDefaultHook creates a new default hook instance
func NewDefaultHook() *DefaultHook {
	return &DefaultHook{}
}

// OnPropose implements the Hook interface for propose operations
func (h *DefaultHook) OnPropose(req *dto.ProposeRequest) bool {
	log.Infof("propose hook on height %d is OK", req.Height)
	return true
}

// OnCommit implements the Hook interface for commit operations
func (h *DefaultHook) OnCommit(req *dto.CommitRequest) bool {
	log.Infof("commit hook on height %d is OK", req.Height)
	return true
}
