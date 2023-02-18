package server

import (
	"context"
	"github.com/vadiminshakov/committer/entity"
	pb "github.com/vadiminshakov/committer/proto"
)

func (s *Server) ProposeHandler(ctx context.Context, req *pb.ProposeRequest) (*pb.Response, error) {
	r := &entity.ProposeRequest{
		Key:    req.Key,
		Value:  req.Value,
		Height: req.Index,
	}
	resp, err := s.committer.Propose(ctx, r)
	if err != nil {
		return nil, err
	}
	respPb := &pb.Response{
		Type:  pb.Type(resp.ResponseType),
		Index: resp.Height,
	}
	return respPb, nil
}

func (s *Server) PrecommitHandler(ctx context.Context, req *pb.PrecommitRequest) (*pb.Response, error) {
	resp, err := s.committer.Precommit(ctx, req.Index)
	if err != nil {
		return nil, err
	}
	respPb := &pb.Response{
		Type:  pb.Type(resp.ResponseType),
		Index: resp.Height,
	}
	return respPb, nil
}

func (s *Server) CommitHandler(ctx context.Context, req *pb.CommitRequest) (*pb.Response, error) {
	r := &entity.CommitRequest{
		Height:     req.Index,
		IsRollback: req.IsRollback,
	}
	resp, err := s.committer.Commit(ctx, r)
	if err != nil {
		return nil, err
	}
	respPb := &pb.Response{
		Type:  pb.Type(resp.ResponseType),
		Index: resp.Height,
	}
	return respPb, nil
}
