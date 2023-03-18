package server

import (
	"context"
	"errors"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/openzipkin/zipkin-go"
	zipkingrpc "github.com/openzipkin/zipkin-go/middleware/grpc"
	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/cohort"
	"github.com/vadiminshakov/committer/core/entity"
	"github.com/vadiminshakov/committer/io/db"
	"github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"google.golang.org/grpc"
	"net"
	"time"
)

type Option func(server *Server) error

const (
	TWO_PHASE   = "two-phase"
	THREE_PHASE = "three-phase"
)

type Coordinator interface {
	Broadcast(ctx context.Context, req entity.BroadcastRequest) (*entity.BroadcastResponse, error)
	Height() uint64
}

// Server holds server instance, node config and connections to followers (if it's a coordinator node)
type Server struct {
	proto.UnimplementedCommitServer
	Addr        string
	GRPCServer  *grpc.Server
	DB          db.Repository
	DBPath      string
	ProposeHook func(req *proto.ProposeRequest) bool
	CommitHook  func(req *proto.CommitRequest) bool
	Tracer      *zipkin.Tracer
	Config      *config.Config
	coordinator Coordinator
	cohort      cohort.Cohort
}

func (s *Server) Propose(ctx context.Context, req *proto.ProposeRequest) (*proto.Response, error) {
	var span zipkin.Span
	if s.Tracer != nil {
		span, ctx = s.Tracer.StartSpanFromContext(ctx, "ProposeHandle")
		defer span.Finish()
	}
	resp, err := s.cohort.Propose(ctx, proposeRequestPbToEntity(req))
	return cohortResponseToProto(resp), err
}

func (s *Server) Precommit(ctx context.Context, req *proto.PrecommitRequest) (*proto.Response, error) {
	var span zipkin.Span
	if s.Tracer != nil {
		span, _ = s.Tracer.StartSpanFromContext(ctx, "PrecommitHandle")
		defer span.Finish()
	}
	resp, err := s.cohort.Precommit(ctx, req.Index, votesPbToEntity(req.Votes))
	return cohortResponseToProto(resp), err
}

func (s *Server) Commit(ctx context.Context, req *proto.CommitRequest) (*proto.Response, error) {
	var span zipkin.Span
	if s.Tracer != nil {
		span, ctx = s.Tracer.StartSpanFromContext(ctx, "CommitHandle")
		defer span.Finish()
	}

	resp, err := s.cohort.Commit(ctx, commitRequestPbToEntity(req))
	return cohortResponseToProto(resp), err
}

func (s *Server) Get(ctx context.Context, req *proto.Msg) (*proto.Value, error) {
	var span zipkin.Span
	if s.Tracer != nil {
		span, ctx = s.Tracer.StartSpanFromContext(ctx, "GetHandle")
		defer span.Finish()
	}

	value, err := s.DB.Get(req.Key)
	if err != nil {
		return nil, err
	}
	return &proto.Value{Value: value}, nil
}

func (s *Server) Put(ctx context.Context, req *proto.Entry) (*proto.Response, error) {
	resp, err := s.coordinator.Broadcast(ctx, entity.BroadcastRequest{
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

func protoToVotes(votes []*proto.Vote) []*entity.Vote {
	pbvotes := make([]*entity.Vote, 0, len(votes))
	for _, v := range votes {
		pbvotes = append(pbvotes, &entity.Vote{
			Node:       v.Node,
			IsAccepted: v.IsAccepted,
		})
	}

	return pbvotes
}

func (s *Server) NodeInfo(ctx context.Context, req *empty.Empty) (*proto.Info, error) {
	var span zipkin.Span
	if s.Tracer != nil {
		span, ctx = s.Tracer.StartSpanFromContext(ctx, "NodeInfoHandle")
		defer span.Finish()
	}

	return &proto.Info{Height: s.cohort.Height()}, nil
}

// New fabric func for Server
func New(conf *config.Config, tracer *zipkin.Tracer, cohort cohort.Cohort, coordinator Coordinator, database db.Repository, opts ...Option) (*Server, error) {
	log.SetFormatter(&log.TextFormatter{
		ForceColors:     true, // Seems like automatic color detection doesn't work on windows terminals
		FullTimestamp:   true,
		TimestampFormat: time.RFC822,
	})

	server := &Server{Addr: conf.Nodeaddr, cohort: cohort, coordinator: coordinator,
		DB: database, Config: conf, Tracer: tracer}
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

	if s.Config.WithTrace {
		s.GRPCServer = grpc.NewServer(grpc.ChainUnaryInterceptor(opts...), grpc.StatsHandler(zipkingrpc.NewServerHandler(s.Tracer)))
	} else {
		s.GRPCServer = grpc.NewServer(grpc.ChainUnaryInterceptor(opts...))
	}
	proto.RegisterCommitServer(s.GRPCServer, s)

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
