package server

import (
	"context"
	"github.com/vadiminshakov/committer/core"
	pb "github.com/vadiminshakov/committer/proto"
	"google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	"log"
	"net"
)

type Server struct {
}

func (s *Server) Propose(ctx context.Context, req *pb.ProposeRequest) (*pb.Response, error) {
	return core.ProposeHandler(ctx, req)
}
func (s *Server) Precommit(ctx context.Context, req *pb.PrecommitRequest) (*pb.Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Precommit not implemented")
}
func (s *Server) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Commit not implemented")
}

func NewCommitServer() *Server {
	return &Server{}
}

func Run(serv *Server) {
	grpcServer := grpc.NewServer()
	pb.RegisterCommitServer(grpcServer, serv)

	l, err := net.Listen("tcp", "localhost:3050")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("Listening on tcp://%s:%s", "localhost", "3050")
	grpcServer.Serve(l)
}
