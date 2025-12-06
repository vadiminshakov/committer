// Package server provides gRPC server implementation for the committer service.
//
// This package implements both internal commit API for node-to-node communication
// and client API for external interactions with the distributed consensus system.
package server

import (
	"context"
	"errors"
	"net"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/core/dto"
	"github.com/vadiminshakov/committer/io/gateway/grpc/proto"
	"github.com/vadiminshakov/committer/io/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	// TWO_PHASE represents the two-phase commit protocol.
	TWO_PHASE = "two-phase"
	// THREE_PHASE represents the three-phase commit protocol.
	THREE_PHASE = "three-phase"
)

// Coordinator defines the interface for coordinator operations.
type Coordinator interface {
	Broadcast(ctx context.Context, req dto.BroadcastRequest) (*dto.BroadcastResponse, error)
	Height() uint64
	SetHeight(height uint64)
}

// Cohort defines the interface for cohort operations.
//
//go:generate mockgen -destination=../../../../mocks/mock_cohort.go -package=mocks . Cohort
type Cohort interface {
	Propose(ctx context.Context, req *dto.ProposeRequest) (*dto.CohortResponse, error)
	Precommit(ctx context.Context, index uint64) (*dto.CohortResponse, error)
	Commit(ctx context.Context, in *dto.CommitRequest) (*dto.CohortResponse, error)
	Abort(ctx context.Context, req *dto.AbortRequest) (*dto.CohortResponse, error)
	Height() uint64
}

// Server holds server instance, node config and connections to followers (if it's a coordinator node).
type Server struct {
	proto.UnimplementedInternalCommitAPIServer
	proto.UnimplementedClientAPIServer

	cohort      Cohort                               // Cohort implementation for this node
	store       *store.Store                         // Persistent storage
	coordinator Coordinator                          // Coordinator implementation (if this node is a coordinator)
	GRPCServer  *grpc.Server                         // gRPC server instance
	Config      *config.Config                       // Node configuration
	ProposeHook func(req *proto.ProposeRequest) bool // Hook for propose phase
	CommitHook  func(req *proto.CommitRequest) bool  // Hook for commit phase
	Addr        string                               // Server address
}

func (s *Server) Propose(ctx context.Context, req *proto.ProposeRequest) (*proto.Response, error) {
	if s.cohort == nil {
		return nil, status.Error(codes.FailedPrecondition, "cohort role not enabled on this node")
	}
	resp, err := s.cohort.Propose(ctx, proposeRequestPbToEntity(req))
	return cohortResponseToProto(resp), err
}

func (s *Server) Precommit(ctx context.Context, req *proto.PrecommitRequest) (*proto.Response, error) {
	if s.cohort == nil {
		return nil, status.Error(codes.FailedPrecondition, "cohort role not enabled on this node")
	}
	resp, err := s.cohort.Precommit(ctx, req.Index)
	return cohortResponseToProto(resp), err
}

func (s *Server) Commit(ctx context.Context, req *proto.CommitRequest) (*proto.Response, error) {
	if s.cohort == nil {
		return nil, status.Error(codes.FailedPrecondition, "cohort role not enabled on this node")
	}
	resp, err := s.cohort.Commit(ctx, commitRequestPbToEntity(req))
	return cohortResponseToProto(resp), err
}

func (s *Server) Abort(ctx context.Context, req *proto.AbortRequest) (*proto.Response, error) {
	if s.cohort == nil {
		return nil, status.Error(codes.FailedPrecondition, "cohort role not enabled on this node")
	}
	abortReq := &dto.AbortRequest{
		Height: req.Height,
		Reason: req.Reason,
	}
	resp, err := s.cohort.Abort(ctx, abortReq)
	return cohortResponseToProto(resp), err
}

func (s *Server) Get(ctx context.Context, req *proto.Msg) (*proto.Value, error) {
	value, err := s.store.Get(req.Key)
	if err != nil {
		return nil, err
	}
	return &proto.Value{Value: value}, nil
}

// Put initiates a distributed transaction to store a key-value pair.
func (s *Server) Put(ctx context.Context, req *proto.Entry) (*proto.Response, error) {
	if s.coordinator == nil {
		return nil, status.Error(codes.FailedPrecondition, "coordinator role not enabled on this node")
	}
	resp, err := s.coordinator.Broadcast(ctx, dto.BroadcastRequest{
		Key:   req.Key,
		Value: req.Value,
	})
	if err != nil {
		return nil, err
	}

	return &proto.Response{
		Type:  proto.Type(resp.Type),
		Index: resp.Height,
	}, nil
}

// NodeInfo returns information about the current node.
func (s *Server) NodeInfo(ctx context.Context, req *emptypb.Empty) (*proto.Info, error) {
	switch {
	case s.cohort != nil:
		return &proto.Info{Height: s.cohort.Height()}, nil
	case s.coordinator != nil:
		return &proto.Info{Height: s.coordinator.Height()}, nil
	default:
		return nil, status.Error(codes.FailedPrecondition, "node has neither cohort nor coordinator role configured")
	}
}

// New creates a new Server instance with the specified configuration.
func New(conf *config.Config, cohort Cohort, coordinator Coordinator, stateStore *store.Store) (*Server, error) {
	log.SetFormatter(&log.TextFormatter{
		ForceColors:     true, // Seems like automatic color detection doesn't work on windows terminals
		FullTimestamp:   true,
		TimestampFormat: time.RFC822,
	})

	server := &Server{
		Addr:        conf.Nodeaddr,
		cohort:      cohort,
		coordinator: coordinator,
		store:       stateStore,
		Config:      conf,
	}

	if server.Config.CommitType == TWO_PHASE {
		log.Info("two-phase-commit mode enabled")
	} else {
		log.Info("three-phase-commit mode enabled")
	}
	err := checkServerFields(server)
	return server, err
}

func checkServerFields(server *Server) error {
	if server.store == nil {
		return errors.New("store is not configured")
	}
	if server.Config.Role == "cohort" && server.cohort == nil {
		return errors.New("cohort role selected but cohort implementation is nil")
	}
	if server.Config.Role == "coordinator" && server.coordinator == nil {
		return errors.New("coordinator role selected but coordinator implementation is nil")
	}
	return nil
}

// Run starts the gRPC server in a non-blocking manner.
func (s *Server) Run(opts ...grpc.UnaryServerInterceptor) {
	var err error
	s.GRPCServer = grpc.NewServer(grpc.ChainUnaryInterceptor(opts...))
	if s.cohort != nil {
		proto.RegisterInternalCommitAPIServer(s.GRPCServer, s)
	}
	proto.RegisterClientAPIServer(s.GRPCServer, s)

	l, err := net.Listen("tcp", s.Addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Infof("listening on tcp://%s", s.Addr)

	go s.GRPCServer.Serve(l)
}

// Stop gracefully stops the gRPC server.
func (s *Server) Stop() {
	log.Info("stopping server")
	s.GRPCServer.GracefulStop()
	if s.store != nil {
		if err := s.store.Close(); err != nil {
			log.Infof("failed to close store: %s\n", err)
		}
	}
	log.Info("server stopped")
}
