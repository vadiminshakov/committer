package hooks

import (
	"github.com/vadiminshakov/committer/hooks/src"
	pb "github.com/vadiminshakov/committer/proto"
	"github.com/vadiminshakov/committer/server"
)

type ProposeHook func(req *pb.ProposeRequest) bool
type CommitHook func(req *pb.CommitRequest) bool

func Get(hooksPath string) ([]server.Option, error) {
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
	return []server.Option{proposeHook(src.Propose), commitHook(src.Commit)}, nil
}
