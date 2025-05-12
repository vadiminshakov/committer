package server

import (
	"context"
	"errors"
	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/cohort"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/io/db"
	"github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"net"
	"time"
)

type Option func(server *Server) error

const (
	TWO_PHASE   = "two-phase"
	THREE_PHASE = "three-phase"
)

type Coordinator interface {
	Broadcast(ctx context.Context, req dto.BroadcastRequest) (*dto.BroadcastResponse, error)
	Height() uint64
}

// Server holds server instance, node config and connections to followers (if it's a coordinator node)
type Server struct {
	proto.UnimplementedInternalCommitAPIServer
	proto.UnimplementedClientAPIServer
	
	cohort      cohort.Cohort
	DB          db.Repository
	coordinator Coordinator
	GRPCServer  *grpc.Server
	Config      *config.Config
	ProposeHook func(req *proto.ProposeRequest) bool
	CommitHook  func(req *proto.CommitRequest) bool
	Addr        string
	DBPath      string
}

func (s *Server) Propose(ctx context.Context, req *proto.ProposeRequest) (*proto.Response, error) {
	resp, err := s.cohort.Propose(ctx, proposeRequestPbToEntity(req))
	return cohortResponseToProto(resp), err
}

func (s *Server) Precommit(ctx context.Context, req *proto.PrecommitRequest) (*proto.Response, error) {
	resp, err := s.cohort.Precommit(ctx, req.Index)
	return cohortResponseToProto(resp), err
}

func (s *Server) Commit(ctx context.Context, req *proto.CommitRequest) (*proto.Response, error) {
	resp, err := s.cohort.Commit(ctx, commitRequestPbToEntity(req))
	return cohortResponseToProto(resp), err
}

func (s *Server) Get(ctx context.Context, req *proto.Msg) (*proto.Value, error) {
	value, err := s.DB.Get(req.Key)
	if err != nil {
		return nil, err
	}
	return &proto.Value{Value: value}, nil
}

func (s *Server) Put(ctx context.Context, req *proto.Entry) (*proto.Response, error) {
	resp, err := s.coordinator.Broadcast(ctx, dto.BroadcastRequest{
		Key:   req.Key,
		Value: req.Value,
	})
	if err != nil {
		return nil, err
	}

	return &proto.Response{
		Type:  proto.Type(resp.Type),
		Index: resp.Index,
	}, nil
}

func (s *Server) NodeInfo(ctx context.Context, req *emptypb.Empty) (*proto.Info, error) {
	return &proto.Info{Height: s.cohort.Height()}, nil
}

// New fabric func for Server
func New(conf *config.Config, cohort cohort.Cohort, coordinator Coordinator, database db.Repository, opts ...Option) (*Server, error) {
	log.SetFormatter(&log.TextFormatter{
		ForceColors:     true, // Seems like automatic color detection doesn't work on windows terminals
		FullTimestamp:   true,
		TimestampFormat: time.RFC822,
	})

	server := &Server{Addr: conf.Nodeaddr, cohort: cohort, coordinator: coordinator,
		DB: database, Config: conf}
	var err error
	for _, option := range opts {
		err = option(server)
		if err != nil {
			return nil, err
		}
	}

	if server.Config.CommitType == TWO_PHASE {
		log.Info("two-phase-commit mode enabled")
	} else {
		log.Info("three-phase-commit mode enabled")
	}
	err = checkServerFields(server)
	return server, err
}

// WithBadgerDB adds BadgerDB manager to the Server instance
func WithBadgerDB(path string) func(*Server) error {
	return func(server *Server) error {
		var err error
		server.DB, err = db.New(path)
		return err
	}
}

func checkServerFields(server *Server) error {
	if server.DB == nil {
		return errors.New("database is not selected")
	}
	return nil
}

// Run starts non-blocking GRPC server
func (s *Server) Run(opts ...grpc.UnaryServerInterceptor) {
	var err error
	s.GRPCServer = grpc.NewServer(grpc.ChainUnaryInterceptor(opts...))
	proto.RegisterInternalCommitAPIServer(s.GRPCServer, s)
	proto.RegisterClientAPIServer(s.GRPCServer, s)

	l, err := net.Listen("tcp", s.Addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Infof("listening on tcp://%s", s.Addr)

	go s.GRPCServer.Serve(l)
}

// Stop stops server
func (s *Server) Stop() {
	log.Info("stopping server")
	s.GRPCServer.GracefulStop()
	if err := s.DB.Close(); err != nil {
		log.Infof("failed to close db, err: %s\n", err)
	}
	log.Info("server stopped")
}
