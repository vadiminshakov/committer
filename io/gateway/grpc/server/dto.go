package server

import (
	"github.com/vadiminshakov/committer/core/entity"
	"github.com/vadiminshakov/committer/io/gateway/grpc/proto"
)

func proposeRequestPbToEntity(request *proto.ProposeRequest) *entity.ProposeRequest {
	if request == nil {
		return nil
	}

	return &entity.ProposeRequest{
		Key:    request.Key,
		Value:  request.Value,
		Height: request.Index,
	}
}

func commitRequestPbToEntity(request *proto.CommitRequest) *entity.CommitRequest {
	if request == nil {
		return nil
	}

	return &entity.CommitRequest{
		Height:     request.Index,
		IsRollback: request.IsRollback,
	}
}

func entityResponseToPb(e *entity.Response) *proto.Response {
	if e == nil {
		return nil
	}
	return &proto.Response{
		Type:  proto.Type(e.ResponseType),
		Index: e.Height,
	}
}

func votesPbToEntity(votes []*proto.Vote) []*entity.Vote {
	if votes == nil {
		return nil
	}

	newVotes := make([]*entity.Vote, 0, len(votes))
	for _, v := range votes {
		newVotes = append(newVotes, &entity.Vote{
			Node:       v.Node,
			IsAccepted: v.IsAccepted,
		})
	}

	return newVotes
}
