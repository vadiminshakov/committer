package helpers

import pb "github.com/vadiminshakov/committer/proto"

type ProposeHook func(req *pb.ProposeRequest) bool
type CommitHook func(req *pb.CommitRequest) bool
