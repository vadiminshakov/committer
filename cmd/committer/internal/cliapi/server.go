// Package cliapi is the CLI API of the committer binary: the CLI writes
// through a coordinator and reads a cohort's committed data. A node started
// with -clientaddr serves it there, apart from the protocol traffic.
package cliapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/vadiminshakov/committer/v2/cmd/committer/internal/cliapi/pb"
	"github.com/vadiminshakov/committer/v2/core/dto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Committer runs a transaction; a coordinator implements it.
type Committer interface {
	Commit(ctx context.Context, key string, value []byte) (uint64, error)
}

// Reader reads a committed value; a cohort's store implements it.
type Reader interface {
	Get(key string) ([]byte, error)
}

// Server serves Put through committer and Get through reader. Either may be
// nil, which rejects the corresponding call.
type Server struct {
	pb.UnimplementedCLIServer

	committer Committer
	reader    Reader
}

// Serve serves the CLI API on addr in the background. The returned function
// stops the server.
func Serve(addr string, committer Committer, reader Reader) (func(), error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}

	srv := grpc.NewServer()
	pb.RegisterCLIServer(srv, &Server{committer: committer, reader: reader})

	slog.Info("CLI API listening", "addr", "tcp://"+addr)

	go func() {
		if err := srv.Serve(listener); err != nil {
			slog.Error("CLI API server failed", "err", err)
		}
	}()

	return srv.GracefulStop, nil
}

// Put runs one transaction on the coordinator.
func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if s.committer == nil {
		return nil, status.Error(codes.FailedPrecondition, "put requires a coordinator")
	}

	height, err := s.committer.Commit(ctx, req.GetKey(), req.GetValue())
	if err != nil {
		return nil, commitErrorToStatus(err)
	}

	return &pb.PutResponse{Height: height}, nil
}

// Get reads a committed value from the cohort's store.
func (s *Server) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if s.reader == nil {
		return nil, status.Error(codes.FailedPrecondition, "get requires a cohort")
	}

	value, err := s.reader.Get(req.GetKey())
	if err != nil {
		return nil, fmt.Errorf("get key %q: %w", req.GetKey(), err)
	}

	return &pb.GetResponse{Value: value}, nil
}

func commitErrorToStatus(err error) error {
	switch {
	// ABORT is durable here even when a deadline caused it: report the outcome.
	case errors.Is(err, dto.ErrAborted):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, dto.ErrInvalidTransaction):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, dto.ErrCoordinatorNotReady):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, dto.ErrPrecommitVote):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
