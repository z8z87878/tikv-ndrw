package standalone_storage

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1). define  StandAloneStorage struct;

	// RaftStorage is an implementation of `Storage` (see tikv/server.go) backed by a Raft node. It is part of a Raft network.
	// By using Raft, reads and writes are consistent with other nodes in the TinyKV instance.
	engines *engine_util.Engines
	config  *config.Config
	wg      sync.WaitGroup
}

//NewStandAloneStorage init
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")

	os.MkdirAll(kvPath, os.ModePerm)

	kvDB := engine_util.CreateDB(kvPath, false)
	engines := engine_util.NewEngines(kvDB, nil, kvPath, "")

	return &StandAloneStorage{engines: engines, config: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engines.Kv.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engines.Kv.NewTransaction(false)
	sr := &StorageRd{
		txn: txn,
	}
	return sr, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			return engine_util.PutCF(s.engines.Kv, put.Cf, put.Key, put.Value) // error : 抛出给业务，让业务处理怎么打log
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			return engine_util.DeleteCF(s.engines.Kv, delete.Cf, delete.Key)
		}
	}
	return nil
}
