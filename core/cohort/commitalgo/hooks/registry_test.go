package hooks

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vadiminshakov/committer/core/dto"
)

// TestHook is a simple test hook implementation
type TestHook struct {
	proposeResult bool
	commitResult  bool
	proposeCalled bool
	commitCalled  bool
}

func (t *TestHook) OnPropose(req *dto.ProposeRequest) bool {
	t.proposeCalled = true
	return t.proposeResult
}

func (t *TestHook) OnCommit(req *dto.CommitRequest) bool {
	t.commitCalled = true
	return t.commitResult
}

func TestRegistry_Register(t *testing.T) {
	registry := NewRegistry()
	hook := &TestHook{}

	require.Equal(t, 0, registry.Count(), "Expected 0 hooks initially")

	registry.Register(hook)

	require.Equal(t, 1, registry.Count(), "Expected 1 hook after registration")
}

func TestRegistry_ExecutePropose(t *testing.T) {
	registry := NewRegistry()

	// Test with hook that returns true
	hook1 := &TestHook{proposeResult: true}
	registry.Register(hook1)

	req := &dto.ProposeRequest{Height: 1, Key: "test", Value: []byte("value")}
	result := registry.ExecutePropose(req)

	require.True(t, result, "Expected propose to succeed")
	require.True(t, hook1.proposeCalled, "Expected hook to be called")

	// Test with hook that returns false
	hook2 := &TestHook{proposeResult: false}
	registry.Register(hook2)

	result = registry.ExecutePropose(req)

	require.False(t, result, "Expected propose to fail when hook returns false")
}

func TestRegistry_ExecuteCommit(t *testing.T) {
	registry := NewRegistry()

	// Test with hook that returns true
	hook1 := &TestHook{commitResult: true}
	registry.Register(hook1)

	req := &dto.CommitRequest{Height: 1}
	result := registry.ExecuteCommit(req)

	require.True(t, result, "Expected commit to succeed")
	require.True(t, hook1.commitCalled, "Expected hook to be called")

	// Test with hook that returns false
	hook2 := &TestHook{commitResult: false}
	registry.Register(hook2)

	result = registry.ExecuteCommit(req)

	require.False(t, result, "Expected commit to fail when hook returns false")
}

func TestRegistry_MultipleHooks(t *testing.T) {
	registry := NewRegistry()

	hook1 := &TestHook{proposeResult: true, commitResult: true}
	hook2 := &TestHook{proposeResult: true, commitResult: true}
	hook3 := &TestHook{proposeResult: false, commitResult: true} // This one fails

	registry.Register(hook1)
	registry.Register(hook2)
	registry.Register(hook3)

	proposeReq := &dto.ProposeRequest{Height: 1, Key: "test", Value: []byte("value")}
	commitReq := &dto.CommitRequest{Height: 1}

	// Propose should fail because hook3 returns false
	require.False(t, registry.ExecutePropose(proposeReq), "Expected propose to fail")

	// Commit should succeed because all hooks return true for commit
	require.True(t, registry.ExecuteCommit(commitReq), "Expected commit to succeed")

	// Check that all hooks were called
	require.True(t, hook1.proposeCalled, "Expected hook1 to be called for propose")
	require.True(t, hook2.proposeCalled, "Expected hook2 to be called for propose")
	require.True(t, hook3.proposeCalled, "Expected hook3 to be called for propose")

	require.True(t, hook1.commitCalled, "Expected hook1 to be called for commit")
	require.True(t, hook2.commitCalled, "Expected hook2 to be called for commit")
	require.True(t, hook3.commitCalled, "Expected hook3 to be called for commit")
}
