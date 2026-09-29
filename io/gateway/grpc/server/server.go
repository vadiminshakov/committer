// Package server provides gRPC server implementation for the committer service.
//
// This package implements both internal commit API for node-to-node communication
// and client API for external interactions with the distributed consensus system.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/vadiminshakov/committer/v2/internal/config"
	corecoordinator "github.com/vadiminshakov/committer/v2/internal/core/coordinator"
	"github.com/vadiminshakov/committer/v2/internal/core/dto"
	"github.com/vadiminshakov/committer/v2/internal/io/gateway/grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	Decision(height uint64) dto.Outcome
}

// Cohort defines the interface for cohort operations.
//
//go:generate mockgen -destination=../../../../mocks/mock_cohort.go -package=mocks . Cohort
type Cohort interface {
	Propose(ctx context.Context, req *dto.ProposeRequest) (*dto.CohortResponse, error)
	Precommit(ctx context.Context, index uint64) (*dto.CohortResponse, error)
	Commit(ctx context.Context, in *dto.CommitRequest) (*dto.CohortResponse, error)
	Abort(ctx context.Context, req *dto.AbortRequest) (*dto.CohortResponse, error)
}

// Reader serves client reads of committed values.
type Reader interface {
	Get(key string) ([]byte, error)
}

// Server holds server instance, node config and connections to followers (if it's a coordinator node).
type Server struct {
	proto.UnimplementedInternalCommitAPIServer
	proto.UnimplementedClientAPIServer

	cohort      Cohort         // Cohort implementation for this node
	reader      Reader         // Serves Get; nil disables reads
	coordinator Coordinator    // Coordinator implementation (if this node is a coordinator)
	GRPCServer  *grpc.Server   // gRPC server instance
	Config      *config.Config // Node configuration
	Addr        string         // Server address
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

// Decision serves the 2PC termination protocol: it returns the coordinator's
// recorded outcome for the given height to an in-doubt cohort.
func (s *Server) Decision(ctx context.Context, req *proto.DecisionRequest) (*proto.DecisionResponse, error) {
	if s.coordinator == nil {
		return nil, status.Error(codes.FailedPrecondition, "coordinator role not enabled on this node")
	}

	outcome := proto.Outcome_OUTCOME_UNKNOWN

	switch s.coordinator.Decision(req.Height) {
	case dto.OutcomeCommit:
		outcome = proto.Outcome_OUTCOME_COMMIT
	case dto.OutcomeAbort:
		outcome = proto.Outcome_OUTCOME_ABORT
	case dto.OutcomeUnknown:
		// Explicitly report undecided heights as unknown.
	}

	return &proto.DecisionResponse{Outcome: outcome}, nil
}

func (s *Server) Get(ctx context.Context, req *proto.Msg) (*proto.Value, error) {
	if s.reader == nil {
		return nil, status.Error(codes.Unimplemented, "this node does not serve reads")
	}

	value, err := s.reader.Get(req.Key)
	if err != nil {
		return nil, fmt.Errorf("get key %q: %w", req.Key, err)
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
		return nil, coordinatorErrorToStatus(err)
	}

	return &proto.Response{
		Type:  proto.Type(resp.Type),
		Index: resp.Height,
	}, nil
}

func coordinatorErrorToStatus(err error) error {
	var committedNotApplied *corecoordinator.CommittedNotAppliedError
	switch {
	// ABORT is durable here even when a deadline caused it: report the outcome.
	case errors.Is(err, corecoordinator.ErrProposeVote):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.As(err, &committedNotApplied):
		return status.Error(codes.Internal, err.Error())
	case errors.Is(err, corecoordinator.ErrInvalidTransaction):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, corecoordinator.ErrCoordinatorNotReady):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, corecoordinator.ErrPrecommitVote):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// New creates a new Server instance with the specified configuration.
// reader may be nil. The server does not own or close it.
func New(conf *config.Config, cohort Cohort, coordinator Coordinator, reader Reader) (*Server, error) {
	server := &Server{
		Addr:        conf.Nodeaddr,
		cohort:      cohort,
		coordinator: coordinator,
		reader:      reader,
		Config:      conf,
	}

	if server.Config.CommitType == TWO_PHASE {
		slog.Info("two-phase-commit mode enabled")
	} else {
		slog.Info("three-phase-commit mode enabled")
	}

	err := checkServerFields(server)

	return server, err
}

func checkServerFields(server *Server) error {
	if server.Config.Role == "cohort" && server.cohort == nil {
		return errors.New("cohort role selected but cohort implementation is nil")
	}

	if server.Config.Role == "coordinator" && server.coordinator == nil {
		return errors.New("coordinator role selected but coordinator implementation is nil")
	}

	return nil
}

// Run binds the listen address and serves in the background.
func (s *Server) Run(opts ...grpc.UnaryServerInterceptor) error {
	listener, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.Addr, err)
	}

	s.GRPCServer = grpc.NewServer(grpc.ChainUnaryInterceptor(opts...))
	proto.RegisterInternalCommitAPIServer(s.GRPCServer, s)
	proto.RegisterClientAPIServer(s.GRPCServer, s)

	slog.Info("listening", "addr", "tcp://"+s.Addr)

	go func() {
		if err := s.GRPCServer.Serve(listener); err != nil {
			slog.Error("gRPC server failed", "err", err)
		}
	}()

	return nil
}

// Stop gracefully stops the gRPC server.
func (s *Server) Stop() {
	slog.Info("stopping server")
	s.GRPCServer.GracefulStop()
	slog.Info("server stopped")
}
