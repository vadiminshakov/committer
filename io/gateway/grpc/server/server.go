// Package server provides gRPC server implementation for the committer service.
//
// It serves the internal commit API that the coordinator and cohorts use to
// talk to each other.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/vadiminshakov/committer/v2/core/dto"
	"github.com/vadiminshakov/committer/v2/io/gateway/grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Coordinator defines the interface for coordinator operations.
type Coordinator interface {
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

// Server serves the internal commit API of one node.
type Server struct {
	proto.UnimplementedInternalCommitAPIServer

	cohort          Cohort
	coordinator     Coordinator
	listenAddr      string
	coordinatorAddr string
	grpc            *grpc.Server
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

func New(listenAddr, coordinatorAddr string, cohort Cohort, coordinator Coordinator) *Server {
	return &Server{
		listenAddr:      listenAddr,
		coordinatorAddr: coordinatorAddr,
		cohort:          cohort,
		coordinator:     coordinator,
	}
}

// Run binds the listen address and serves in the background.
func (s *Server) Run() error {
	listener, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.listenAddr, err)
	}

	s.grpc = grpc.NewServer(grpc.UnaryInterceptor(s.coordinatorCheck))
	proto.RegisterInternalCommitAPIServer(s.grpc, s)

	slog.Info("listening", "addr", "tcp://"+s.listenAddr)

	go func() {
		if err := s.grpc.Serve(listener); err != nil {
			slog.Error("gRPC server failed", "err", err)
		}
	}()

	return nil
}

// Stop gracefully stops the gRPC server.
func (s *Server) Stop() {
	s.grpc.GracefulStop()
}
