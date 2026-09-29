package server

import (
	"github.com/vadiminshakov/committer/v2/internal/core/dto"
	"github.com/vadiminshakov/committer/v2/internal/io/gateway/grpc/proto"
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

func cohortResponseToProto(response *dto.CohortResponse) *proto.Response {
	if response == nil {
		return nil
	}

	return &proto.Response{
		Type:   proto.Type(response.ResponseType),
		Index:  response.Height,
		Reason: response.Reason,
	}
}
