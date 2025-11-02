package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := mvccTxn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	if lock != nil {
		if lock.Ts <= req.Version {
			return &kvrpcpb.GetResponse{
				Error: &kvrpcpb.KeyError{
					Locked: lock.Info(req.Key),
				},
			}, nil
		}
	}

	val, err := mvccTxn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}

	notFound := val == nil
	return &kvrpcpb.GetResponse{
		Value:    val,
		NotFound: notFound,
	}, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	// Check locks and conflicts first.
	var keyErrs []*kvrpcpb.KeyError
	for _, mutation := range req.Mutations {
		lock, err := mvccTxn.GetLock(mutation.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			keyErrs = append(keyErrs, &kvrpcpb.KeyError{
				Locked: lock.Info(mutation.Key),
			})
		}

		write, ts, err := mvccTxn.MostRecentWrite(mutation.Key)
		if err != nil {
			return nil, err
		}

		if write != nil && ts >= req.StartVersion {
			keyErrs = append(keyErrs, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: ts,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock,
				},
			})
		}
	}

	if len(keyErrs) > 0 {
		return &kvrpcpb.PrewriteResponse{
			Errors: keyErrs,
		}, nil
	}

	// Mutations.
	for _, mutation := range req.Mutations {
		if mutation.GetOp() == kvrpcpb.Op_Put {
			mvccTxn.PutValue(mutation.GetKey(), mutation.GetValue())
			mvccTxn.PutLock(mutation.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      req.StartVersion,
				Kind:    mvcc.WriteKindPut,
				Ttl:     req.LockTtl,
			})
		} else if mutation.GetOp() == kvrpcpb.Op_Del {

		} else if mutation.GetOp() == kvrpcpb.Op_Rollback {

		}
	}

	if err := server.storage.Write(req.Context, mvccTxn.Writes()); err != nil {
		return nil, err
	}
	return &kvrpcpb.PrewriteResponse{}, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	mvccTxn := mvcc.NewMvccTxn(reader, req.StartVersion)
	// Find the lock
	for _, key := range req.Keys {
		lock, err := mvccTxn.GetLock(key)
		if err != nil {
			return nil, err
		}

		if lock == nil {
			write, _, err := mvccTxn.MostRecentWrite(key)
			if err != nil {
				return nil, err
			}

			if write != nil && write.StartTS == req.StartVersion {
				if write.Kind == mvcc.WriteKindRollback {
					// already rolled back -> return failure
					return &kvrpcpb.CommitResponse{
						Error: &kvrpcpb.KeyError{
							Abort: "already rolled back",
						},
					}, nil
				} else if write.Kind == mvcc.WriteKindPut {
					// no lock, but duplicate commit -> return success
					return &kvrpcpb.CommitResponse{}, nil
				}
			}

			if write == nil {
				// no lock, no write -> (lost prewrite?) controversial. return ok
				return &kvrpcpb.CommitResponse{}, nil
			}

			return &kvrpcpb.CommitResponse{
				Error: &kvrpcpb.KeyError{
					Abort: "lock not found",
				},
			}, nil
		}

		if lock.Ts != req.StartVersion {
			return &kvrpcpb.CommitResponse{
				Error: &kvrpcpb.KeyError{
					Retryable: "wrong lock",
				},
			}, nil
		}

		mvccTxn.DeleteLock(key)
		mvccTxn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKind(lock.Kind),
		})
	}

	if err := server.storage.Write(req.Context, mvccTxn.Writes()); err != nil {
		return nil, err
	}
	return &kvrpcpb.CommitResponse{}, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
