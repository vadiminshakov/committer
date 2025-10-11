package hooks

import (
	"github.com/vadiminshakov/committer/core/dto"
)

// Hook defines the interface for commit algorithm hooks.
type Hook interface {
	OnPropose(req *dto.ProposeRequest) bool
	OnCommit(req *dto.CommitRequest) bool
}

// Registry manages a collection of hooks.
type Registry struct {
	hooks []Hook
}

// NewRegistry creates a new hook registry.
func NewRegistry() *Registry {
	return &Registry{
		hooks: make([]Hook, 0),
	}
}

// Register adds a new hook to the registry.
func (r *Registry) Register(hook Hook) {
	r.hooks = append(r.hooks, hook)
}

// ExecutePropose runs all registered propose hooks.
// Returns false if any hook returns false.
func (r *Registry) ExecutePropose(req *dto.ProposeRequest) bool {
	for _, hook := range r.hooks {
		if !hook.OnPropose(req) {
			return false
		}
	}
	return true
}

// ExecuteCommit runs all registered commit hooks.
// Returns false if any hook returns false.
func (r *Registry) ExecuteCommit(req *dto.CommitRequest) bool {
	for _, hook := range r.hooks {
		if !hook.OnCommit(req) {
			return false
		}
	}
	return true
}

// Count returns the number of registered hooks
func (r *Registry) Count() int {
	return len(r.hooks)
}
