package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).

	reader, err := server.storage.Reader(&kvrpcpb.Context{})
	if err != nil {
		return nil, err
	}

	bytes, err := reader.GetCF(req.Cf, req.Key)
	// key doesn't exist
	if bytes == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}

	return &kvrpcpb.RawGetResponse{Value: bytes}, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	err := server.storage.Write(
		&kvrpcpb.Context{},
		[]storage.Modify{
			{Data: storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}},
		},
	)
	return &kvrpcpb.RawPutResponse{}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	err := server.storage.Write(
		&kvrpcpb.Context{},
		[]storage.Modify{
			{Data: storage.Delete{Key: req.Key, Cf: req.Cf}},
		},
	)
	return &kvrpcpb.RawDeleteResponse{}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(&kvrpcpb.Context{})
	if err != nil {
		return nil, err
	}

	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)

	res := []*kvrpcpb.KvPair{}
	for iter.Valid() && len(res) < int(req.Limit) {
		it := iter.Item()
		k := make([]byte, 0, len(it.Key()))
		k = it.KeyCopy(k)

		v := make([]byte, 0, it.ValueSize())
		v, err := it.ValueCopy(v)
		if err != nil {
			return nil, err
		}
		res = append(res, &kvrpcpb.KvPair{Key: k, Value: v})
		iter.Next()
	}

	return &kvrpcpb.RawScanResponse{
		Kvs: res,
	}, nil
}
