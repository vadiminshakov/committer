package hooks

import (
	"github.com/vadiminshakov/committer/entity"
)

type ProposeHook func(req *entity.ProposeRequest) bool
type CommitHook func(req *entity.CommitRequest) bool

//func Get() ([]server.Option, error) {
//	proposeHook := func(f ProposeHook) func(*server.Server) error {
//		return func(server *server.Server) error {
//			server.ProposeHook = f
//			return nil
//		}
//	}
//	commitHook := func(f CommitHook) func(*server.Server) error {
//		return func(server *server.Server) error {
//			server.CommitHook = f
//			return nil
//		}
//	}
//	return []server.Option{proposeHook(src.Propose), commitHook(src.Commit)}, nil
//}
