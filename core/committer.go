package core

import (
	"context"
	"errors"
	"fmt"
	"github.com/vadiminshakov/committer/cache"
	"github.com/vadiminshakov/committer/db"
	"github.com/vadiminshakov/committer/helpers"
	pb "github.com/vadiminshakov/committer/proto"
	"log"
)

func ProposeHandler(ctx context.Context, req *pb.ProposeRequest, hook helpers.ProposeHook, nodecache *cache.Cache) (*pb.Response, error) {
	var response *pb.Response

	if hook(req) {
		log.Printf("Received: %s=%s\n", req.Key, string(req.Value))
		nodecache.Set(req.Index, req.Key, req.Value)
		response = &pb.Response{Type: pb.Type_ACK}
	} else {
		response = &pb.Response{Type: pb.Type_NACK}
	}
	return response, nil
}

func PrecommitHandler(ctx context.Context, req *pb.PrecommitRequest) (*pb.Response, error) {
	return &pb.Response{Type: pb.Type_ACK}, nil
}

func CommitHandler(ctx context.Context, req *pb.CommitRequest, hook helpers.CommitHook, db db.Database, nodecache *cache.Cache) (*pb.Response, error) {
	var response *pb.Response
	if hook(req) {
		log.Printf("Committing on height: %d\n", req.Index)
		key, value, ok := nodecache.Get(req.Index)
		if !ok {
			return &pb.Response{Type: pb.Type_NACK}, errors.New(fmt.Sprintf("no value in node cache on the index %d", req.Index))
		}

		if err := db.Put(key, value); err != nil {
			return nil, err
		}
		response = &pb.Response{Type: pb.Type_ACK}
	} else {
		response = &pb.Response{Type: pb.Type_NACK}
	}
	return response, nil
}
