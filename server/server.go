package server

import (
	"context"
	"errors"
	"github.com/vadiminshakov/committer/cache"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core"
	"github.com/vadiminshakov/committer/db"
	"github.com/vadiminshakov/committer/helpers"
	"github.com/vadiminshakov/committer/peer"
	pb "github.com/vadiminshakov/committer/proto"
	"google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	"log"
	"net"
	"sync/atomic"
)

type Option func(server *Server) error

// Server holds server instance, node config and connections to followers (if it's a coordinator node)
type Server struct {
	Addr        string
	Followers   []*peer.CommitClient
	Config      *config.Config
	GRPCServer  *grpc.Server
	DB          db.Database
	ProposeHook helpers.ProposeHook
	CommitHook  helpers.CommitHook
	NodeCache   *cache.Cache
	Height      uint64
}

func (s *Server) Propose(ctx context.Context, req *pb.ProposeRequest) (*pb.Response, error) {
	return core.ProposeHandler(ctx, req, s.ProposeHook, s.NodeCache)
}
func (s *Server) Precommit(ctx context.Context, req *pb.PrecommitRequest) (*pb.Response, error) {
	return core.PrecommitHandler(ctx, req)
}
func (s *Server) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.Response, error) {
	return core.CommitHandler(ctx, req, s.CommitHook, s.DB, s.NodeCache)
}

func (s *Server) Put(ctx context.Context, req *pb.Entry) (*pb.Response, error) {
	var (
		response *pb.Response
		err      error
	)

	// propose
	s.NodeCache.Set(s.Height, req.Key, req.Value)
	for _, follower := range s.Followers {
		response, err = follower.Propose(ctx, &pb.ProposeRequest{Key: req.Key,
			Value:      req.Value,
			CommitType: pb.CommitType_TWO_PHASE_COMMIT,
			Index:      s.Height})
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if response.Type != pb.Type_ACK {
			return nil, status.Error(codes.Internal, "follower not acknowledged msg")
		}
	}

	// commit
	for _, follower := range s.Followers {
		response, err = follower.Commit(ctx, &pb.CommitRequest{Index: s.Height})
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if response.Type != pb.Type_ACK {
			return nil, status.Error(codes.Internal, "follower not acknowledged msg")
		}
	}
	key, value, ok := s.NodeCache.Get(s.Height)
	if !ok {
		return nil, status.Error(codes.Internal, "can't to find msg in the coordinator's cache")
	}
	if err = s.DB.Put(key, value); err != nil {
		return &pb.Response{Type: pb.Type_NACK}, status.Error(codes.Internal, "failed to save msg on coordinator")
	}

	atomic.AddUint64(&s.Height, 1)

	return &pb.Response{Type: pb.Type_ACK}, nil
}

// NewCommitServer fabric func for Server
func NewCommitServer(addr string, opts ...Option) (*Server, error) {
	server := &Server{Addr: addr}
	var err error
	for _, option := range opts {
		err = option(server)
		if err != nil {
			return nil, err
		}
	}
	server.NodeCache = cache.New()

	err = checkServerFields(server)
	return server, err
}

// WithFollowers creates network connections to followers and adds them to the Server instance
func WithFollowers(followers []string) func(*Server) error {
	return func(server *Server) error {
		for _, node := range followers {
			cli, err := peer.New(node)
			if err != nil {
				return err
			}
			server.Followers = append(server.Followers, cli)
		}
		return nil
	}
}

// WithConfig adds Config instance to the Server instance
func WithConfig(conf *config.Config) func(*Server) error {
	return func(server *Server) error {
		server.Config = conf
		if conf.Role == "coordinator" {
			server.Config.Coordinator = server.Addr
		}
		return nil
	}
}

// WithBadgerDB adds BadgerDB manager to the Server instance
func WithBadgerDB(path string) func(*Server) error {
	return func(server *Server) error {
		var err error
		server.DB, err = db.New(path)
		return err
	}
}

// WithProposeHook adds hook function for Propose stage to the Server instance
func WithProposeHook(f helpers.ProposeHook) func(*Server) error {
	return func(server *Server) error {
		server.ProposeHook = f
		return nil
	}
}

// WithCommitHook adds hook function for Commit stage to the Server instance
func WithCommitHook(f helpers.CommitHook) func(*Server) error {
	return func(server *Server) error {
		server.CommitHook = f
		return nil
	}
}

func checkServerFields(server *Server) error {
	if server.DB == nil {
		return errors.New("database is not selected")
	}
	return nil
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
	if err := s.DB.Close(); err != nil {
		log.Printf("failed to close db, err: %s\n", err)
	}
	log.Println("Server stopped")
}
