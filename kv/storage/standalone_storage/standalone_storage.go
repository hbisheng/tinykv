package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	kvBadger *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvBadger := engine_util.CreateDB(conf.DBPath, false /* raft */)
	return &StandAloneStorage{
		kvBadger: kvBadger,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.kvBadger.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.kvBadger.NewTransaction(false)
	return &BadgerStorageReader{txn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := &engine_util.WriteBatch{}
	for _, m := range batch {
		wb.SetCF(m.Cf(), m.Key(), m.Value())
	}
	wb.WriteToDB(s.kvBadger)
	return nil
}

type BadgerStorageReader struct {
	txn *badger.Txn
}

func (b *BadgerStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	res, err := engine_util.GetCFFromTxn(b.txn, cf, key)
	// If the key is not found, return empty response
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	return res, err
}
func (b *BadgerStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, b.txn)
}
func (b *BadgerStorageReader) Close() {
	b.txn.Discard()
}
