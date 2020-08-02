package core

import (
	"context"
	pb "github.com/vadiminshakov/committer/proto"
)

func ProposeHandler(ctx context.Context, req *pb.ProposeRequest) (*pb.Response, error) {
	if req.CommitType == pb.CommitType_TWO_PHASE_COMMIT {

	} else {

	}
	return &pb.Response{Type: pb.Type_ACK}, nil
}
