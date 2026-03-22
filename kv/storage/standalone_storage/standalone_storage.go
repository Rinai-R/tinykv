package standalone_storage

import (
	"errors"
	"os"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	config config.Config
	db     *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	opt := badger.DefaultOptions
	if err := os.MkdirAll(conf.DBPath, 0777); err != nil {
		log.Errorf("fail to create dir %v, error %v", opt.Dir, err)
		return nil
	}
	opt.Dir = conf.DBPath
	opt.ValueDir = conf.DBPath
	db, err := badger.Open(opt)
	if err != nil {
		log.Errorf("fail to open db: %v", err)
	}
	impl := &StandAloneStorage{
		config: *conf,
		db:     db,
	}
	return impl
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return NewStandAloneReader(ctx, s)
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()
	for _, m := range batch {
		var err error
		switch m.Data.(type) {
		case storage.Put:
			err = txn.Set(engine_util.KeyWithCF(m.Cf(), m.Key()), m.Value())
		case storage.Delete:
			err = txn.Delete(engine_util.KeyWithCF(m.Cf(), m.Key()))
		}
		if err != nil {
			log.Errorf("fail to modify %v, %v, %v, error: %v", m.Cf(), m.Key(), m.Value(), err)
			return err
		}
	}
	return txn.Commit()
}

type StandAloneReader struct {
	txn       *badger.Txn
	ctx       *kvrpcpb.Context
	storage   *StandAloneStorage
	iterCount int
}

func NewStandAloneReader(ctx *kvrpcpb.Context, Storage *StandAloneStorage) (storage.StorageReader, error) {
	return &StandAloneReader{
		txn: Storage.db.NewTransaction(false),
	}, nil
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	return val, err
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneReader) Close() {
	r.txn.Discard()
}
