package core

import (
	"context"
	pb "github.com/vadiminshakov/committer/proto"
	"log"
)

func ProposeHandler(ctx context.Context, req *pb.ProposeRequest) (*pb.Response, error) {
	if req.CommitType == pb.CommitType_TWO_PHASE_COMMIT {

	} else {

	}
	log.Printf("Received: %s=%s\n", req.Key, string(req.Value))
	return &pb.Response{Type: pb.Type_ACK}, nil
}
