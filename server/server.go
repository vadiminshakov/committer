package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/openzipkin/zipkin-go"
	zipkingrpc "github.com/openzipkin/zipkin-go/middleware/grpc"
	log "github.com/sirupsen/logrus"
	"github.com/vadiminshakov/committer/cache"
	"github.com/vadiminshakov/committer/config"
	"github.com/vadiminshakov/committer/db"
	"github.com/vadiminshakov/committer/entity"
	"github.com/vadiminshakov/committer/peer"
	pb "github.com/vadiminshakov/committer/proto"
	"github.com/vadiminshakov/committer/trace"
	"google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	status "google.golang.org/grpc/status"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Option func(server *Server) error

const (
	TWO_PHASE   = "two-phase"
	THREE_PHASE = "three-phase"
)

type committer interface {
	Propose(ctx context.Context, req *entity.ProposeRequest, opts ...grpc.CallOption) (*entity.Response, error)
	Precommit(ctx context.Context, height uint64, opts ...grpc.CallOption) (*entity.Response, error)
	Commit(ctx context.Context, in *entity.CommitRequest, opts ...grpc.CallOption) (*entity.Response, error)
}

// Server holds server instance, node config and connections to followers (if it's a coordinator node)
type Server struct {
	pb.UnimplementedCommitServer
	committer            committer
	Addr                 string
	Followers            []*peer.CommitClient
	Config               *config.Config
	GRPCServer           *grpc.Server
	DB                   db.Database
	DBPath               string
	ProposeHook          func(req *pb.ProposeRequest) bool
	CommitHook           func(req *pb.CommitRequest) bool
	NodeCache            *cache.Cache
	Height               uint64
	cancelCommitOnHeight map[uint64]bool
	mu                   sync.RWMutex
	Tracer               *zipkin.Tracer
}

func (s *Server) Propose(ctx context.Context, req *pb.ProposeRequest) (*pb.Response, error) {
	var span zipkin.Span
	if s.Tracer != nil {
		span, ctx = s.Tracer.StartSpanFromContext(ctx, "ProposeHandle")
		defer span.Finish()
	}
	s.SetProgressForCommitPhase(req.Index, false)
	return s.ProposeHandler(ctx, req)
}

func (s *Server) Precommit(ctx context.Context, req *pb.PrecommitRequest) (*pb.Response, error) {
	var span zipkin.Span
	if s.Tracer != nil {
		span, _ = s.Tracer.StartSpanFromContext(ctx, "PrecommitHandle")
		defer span.Finish()
	}
	if s.Config.CommitType == THREE_PHASE {
		ctx, _ = context.WithTimeout(context.Background(), time.Duration(s.Config.Timeout)*time.Millisecond)
		go func(ctx context.Context) {
		ForLoop:
			for {
				select {
				case <-ctx.Done():
					md := metadata.Pairs("mode", "autocommit")
					ctx := metadata.NewOutgoingContext(context.Background(), md)
					if !s.HasProgressForCommitPhase(req.Index) {
						s.Commit(ctx, &pb.CommitRequest{Index: s.Height})
						log.Warn("commit without coordinator after timeout")
					}
					break ForLoop
				}
			}
		}(ctx)
	}
	return s.PrecommitHandler(ctx, req)
}

func (s *Server) Commit(ctx context.Context, req *pb.CommitRequest) (resp *pb.Response, err error) {
	var span zipkin.Span
	if s.Tracer != nil {
		span, ctx = s.Tracer.StartSpanFromContext(ctx, "CommitHandle")
		defer span.Finish()
	}

	if s.Config.CommitType == THREE_PHASE {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.Internal, "no metadata")
		}

		meta := md["mode"]

		if len(meta) == 0 {
			s.SetProgressForCommitPhase(s.Height, true) // set flag for cancelling 'commit without coordinator' action, coz coordinator responded actually
			resp, err = s.CommitHandler(ctx, req)
			if err != nil {
				return nil, err
			}
			if resp.Type == pb.Type_ACK {
				atomic.AddUint64(&s.Height, 1)
			}
			return
		}

		if req.IsRollback {
			s.rollback()
		}
	} else {
		resp, err = s.CommitHandler(ctx, req)
		if err != nil {
			return
		}

		if resp.Type == pb.Type_ACK {
			atomic.AddUint64(&s.Height, 1)
		}
		return
	}
	return
}

func (s *Server) Get(ctx context.Context, req *pb.Msg) (*pb.Value, error) {
	var span zipkin.Span
	if s.Tracer != nil {
		span, ctx = s.Tracer.StartSpanFromContext(ctx, "GetHandle")
		defer span.Finish()
	}

	value, err := s.DB.Get(req.Key)
	if err != nil {
		return nil, err
	}
	return &pb.Value{Value: value}, nil
}

func (s *Server) Put(ctx context.Context, req *pb.Entry) (*pb.Response, error) {
	var (
		response *pb.Response
		err      error
		span     zipkin.Span
	)

	if s.Tracer != nil {
		span, ctx = s.Tracer.StartSpanFromContext(ctx, "PutHandle")
		defer span.Finish()
	}

	var ctype pb.CommitType
	if s.Config.CommitType == THREE_PHASE {
		ctype = pb.CommitType_THREE_PHASE_COMMIT
	} else {
		ctype = pb.CommitType_TWO_PHASE_COMMIT
	}

	// propose
	log.Infof("propose key %s", req.Key)
	for _, follower := range s.Followers {
		if s.Config.CommitType == THREE_PHASE {
			ctx, _ = context.WithTimeout(ctx, time.Duration(s.Config.Timeout)*time.Millisecond)
		}
		if s.Tracer != nil {
			span, ctx = s.Tracer.StartSpanFromContext(ctx, "Propose")
		}
		var (
			response *pb.Response
			err      error
		)
		for response == nil || response != nil && response.Type == pb.Type_NACK {
			response, err = follower.Propose(ctx, &pb.ProposeRequest{Key: req.Key,
				Value:      req.Value,
				CommitType: ctype,
				Index:      s.Height})
			if s.Tracer != nil && span != nil {
				span.Finish()
			}
			if response != nil && response.Index > s.Height {
				log.Warnf("сoordinator has stale height [%d], update to [%d] and try to send again", s.Height, response.Index)
				s.Height = response.Index
			}
		}
		if err != nil {
			log.Errorf(err.Error())
			return &pb.Response{Type: pb.Type_NACK}, nil
		}
		s.NodeCache.Set(s.Height, req.Key, req.Value)
		if response.Type != pb.Type_ACK {
			return nil, status.Error(codes.Internal, "follower not acknowledged msg")
		}
	}

	// precommit phase only for three-phase mode
	log.Infof("precommit key %s", req.Key)
	for _, follower := range s.Followers {
		if s.Config.CommitType == THREE_PHASE {
			ctx, _ = context.WithTimeout(ctx, time.Duration(s.Config.Timeout)*time.Millisecond)
		}
		if s.Tracer != nil {
			span, ctx = s.Tracer.StartSpanFromContext(ctx, "Precommit")
		}
		response, err = follower.Precommit(ctx, &pb.PrecommitRequest{Index: s.Height})
		if s.Tracer != nil && span != nil {
			span.Finish()
		}
		if err != nil {
			log.Errorf(err.Error())
			return &pb.Response{Type: pb.Type_NACK}, nil
		}
		if response.Type != pb.Type_ACK {
			return nil, status.Error(codes.Internal, "follower not acknowledged msg")
		}
	}

	// the coordinator got all the answers, so it's time to persist msg and send commit command to followers
	key, value, ok := s.NodeCache.Get(s.Height)
	if !ok {
		return nil, status.Error(codes.Internal, "can't to find msg in the coordinator's cache")
	}
	if err = s.DB.Put(key, value); err != nil {
		return &pb.Response{Type: pb.Type_NACK}, status.Error(codes.Internal, "failed to save msg on coordinator")
	}

	// commit
	log.Infof("commit %s", req.Key)
	for _, follower := range s.Followers {
		if s.Tracer != nil {
			span, ctx = s.Tracer.StartSpanFromContext(ctx, "Commit")
		}
		response, err = follower.Commit(ctx, &pb.CommitRequest{Index: s.Height})
		if s.Tracer != nil && span != nil {
			span.Finish()
		}
		if err != nil {
			log.Errorf(err.Error())
			return &pb.Response{Type: pb.Type_NACK}, nil
		}
		if response.Type != pb.Type_ACK {
			return nil, status.Error(codes.Internal, "follower not acknowledged msg")
		}
	}
	log.Infof("committed key %s", req.Key)
	// increase height for next round
	atomic.AddUint64(&s.Height, 1)

	return &pb.Response{Type: pb.Type_ACK}, nil
}

func (s *Server) NodeInfo(ctx context.Context, req *empty.Empty) (*pb.Info, error) {
	var span zipkin.Span
	if s.Tracer != nil {
		span, ctx = s.Tracer.StartSpanFromContext(ctx, "NodeInfoHandle")
		defer span.Finish()
	}

	return &pb.Info{Height: s.Height}, nil
}

// NewCommitServer fabric func for Server
func NewCommitServer(conf *config.Config, committer committer, database db.Database, nodecache *cache.Cache, opts ...Option) (*Server, error) {
	log.SetFormatter(&log.TextFormatter{
		ForceColors:     true, // Seems like automatic color detection doesn't work on windows terminals
		FullTimestamp:   true,
		TimestampFormat: time.RFC822,
	})

	server := &Server{Addr: conf.Nodeaddr, committer: committer, DB: database, NodeCache: nodecache}
	var err error
	for _, option := range opts {
		err = option(server)
		if err != nil {
			return nil, err
		}
	}

	if conf.WithTrace {
		// get Zipkin tracer
		server.Tracer, err = trace.Tracer(fmt.Sprintf("%s:%s", conf.Role, conf.Nodeaddr), conf.Nodeaddr)
		if err != nil {
			return nil, err
		}
	}

	for _, node := range conf.Followers {
		cli, err := peer.New(node, server.Tracer)
		if err != nil {
			return nil, err
		}
		server.Followers = append(server.Followers, cli)
	}

	server.Config = conf
	if conf.Role == "coordinator" {
		server.Config.Coordinator = server.Addr
	}

	server.cancelCommitOnHeight = map[uint64]bool{}

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
	pb.RegisterCommitServer(s.GRPCServer, s)

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

func (s *Server) rollback() {
	s.NodeCache.Delete(s.Height)
}

func (s *Server) SetProgressForCommitPhase(height uint64, docancel bool) {
	s.mu.Lock()
	s.cancelCommitOnHeight[height] = docancel
	s.mu.Unlock()
}

func (s *Server) HasProgressForCommitPhase(height uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cancelCommitOnHeight[height]
}
