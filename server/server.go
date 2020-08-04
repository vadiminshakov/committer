package server

import (
	"context"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core"
	"github.com/vadiminshakov/committer/peer"
	pb "github.com/vadiminshakov/committer/proto"
	"google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	"log"
	"net"
)

type Option func(server *Server)

// Server holds server instance, node config and connections to followers (if it's a coordinator node)
type Server struct {
	Addr       string
	Followers  []*peer.CommitClient
	Config     *config.Config
	GRPCServer *grpc.Server
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

func (s *Server) Put(ctx context.Context, req *pb.Entry) (*pb.Response, error) {
	var (
		response *pb.Response
		err      error
	)
	for _, follower := range s.Followers {
		response, err = follower.Propose(&pb.ProposeRequest{Key: req.Key,
			Value:      req.Value,
			CommitType: pb.CommitType_TWO_PHASE_COMMIT,
			Index:      1})
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if response.Type != pb.Type_ACK {
			return nil, status.Error(codes.Internal, "follower not acknowledged msg")
		}
	}
	return &pb.Response{Type: pb.Type_ACK}, nil
}

// NewCommitServer fabric func for Server
func NewCommitServer(addr string, opts ...Option) (*Server, error) {
	server := &Server{Addr: addr}
	for _, option := range opts {
		option(server)
	}
	return server, nil
}

// WithFollowers creates network connections to followers and adds them to the Server instance
func WithFollowers(followers []string) func(*Server) {
	return func(server *Server) {
		for _, node := range followers {
			cli, err := peer.New(node)
			if err != nil {
				panic(err)
			}
			server.Followers = append(server.Followers, cli)
		}
	}
}

// WithConfig adds Config instance to the Server instance
func WithConfig(conf *config.Config) func(*Server) {
	return func(server *Server) {
		server.Config = conf
		if conf.Role == "coordinator" {
			server.Config.Coordinator = server.Addr
		}
	}
}

// Run starts non-blocking GRPC server
func (s *Server) Run() {
	s.GRPCServer = grpc.NewServer(WithWhitelistChecker())
	pb.RegisterCommitServer(s.GRPCServer, s)

	l, err := net.Listen("tcp", s.Addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("Listening on tcp://%s", s.Addr)

	go s.GRPCServer.Serve(l)
}

// Stop stops server
func (s *Server) Stop() {
	log.Println("Stopping server")
	s.GRPCServer.GracefulStop()
	log.Println("Server stopped")
}
