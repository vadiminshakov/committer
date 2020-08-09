package hooks

import (
	"errors"
	pb "github.com/vadiminshakov/committer/proto"
	"github.com/vadiminshakov/committer/server"
	"plugin"
)

type ProposeHook func(req *pb.ProposeRequest) bool
type CommitHook func(req *pb.CommitRequest) bool

func Get(hooksPath string) ([]server.Option, error) {
	plug, err := plugin.Open(hooksPath)
	if err != nil {
		return nil, err
	}

	WithProposeHook, err := plug.Lookup("Propose")
	if err != nil {
		return nil, err
	}
	proposeFunc, ok := WithProposeHook.(func(req *pb.ProposeRequest) bool)
	if !ok {
		return nil, errors.New("failed to assert WithProposeHook to server.Option")
	}
	WithCommitHook, err := plug.Lookup("Commit")
	if err != nil {
		return nil, err
	}
	commitFunc, ok := WithCommitHook.(func(req *pb.CommitRequest) bool)
	if !ok {
		return nil, errors.New("failed to assert WithCommitHook to server.Option")
	}

	proposeHook := func(f ProposeHook) func(*server.Server) error {
		return func(server *server.Server) error {
			server.ProposeHook = f
			return nil
		}
	}
	commitHook := func(f CommitHook) func(*server.Server) error {
		return func(server *server.Server) error {
			server.CommitHook = f
			return nil
		}
	}

	return []server.Option{proposeHook(proposeFunc), commitHook(commitFunc)}, nil
}
