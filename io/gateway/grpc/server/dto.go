package server

import (
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/io/gateway/grpc/proto"
)

func proposeRequestPbToEntity(request *proto.ProposeRequest) *dto.ProposeRequest {
	if request == nil {
		return nil
	}

	return &dto.ProposeRequest{
		Key:    request.Key,
		Value:  request.Value,
		Height: request.Index,
	}
}

func commitRequestPbToEntity(request *proto.CommitRequest) *dto.CommitRequest {
	if request == nil {
		return nil
	}

	return &dto.CommitRequest{
		Height:     request.Index,
		IsRollback: request.IsRollback,
	}
}

func cohortResponseToProto(e *dto.CohortResponse) *proto.Response {
	if e == nil {
		return nil
	}
	return &proto.Response{
		Type:  proto.Type(e.ResponseType),
		Index: e.Height,
	}
}
