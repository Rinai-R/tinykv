package server

import (
	"context"
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		log.Errorf("fail to get value, error %v", err)
		return nil, err
	}
	resp := &kvrpcpb.RawGetResponse{}
	if value == nil {
		resp.NotFound = true
	} else {
		resp.Value = value
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	err := server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.GetCf(),
				Key:   req.GetKey(),
				Value: req.GetValue(),
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	err := server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.GetCf(),
				Key: req.GetKey(),
			},
		},
	})
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		log.Errorf("fail to del value, error %v", err)
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	iter := reader.IterCF(req.GetCf())
	defer iter.Close()

	kvs := []*kvrpcpb.KvPair{}
	for iter.Seek(req.StartKey); iter.Valid() && uint32(len(kvs)) < req.Limit; iter.Next() {
		item := iter.Item()
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		})
	}

	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}

func KVsExtract(item engine_util.DBItem) (*kvrpcpb.KvPair, error) {
	key := item.Key()
	value, err := item.Value()
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.KvPair{
		Key:   key,
		Value: value,
	}, nil
}
