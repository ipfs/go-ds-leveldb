package leveldb

import (
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/jbenet/goprocess"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type datastore struct {
	DB *leveldb.DB
}

type Options opt.Options

func NewDatastore(path string, opts *Options) (*datastore, error) {
	var nopts opt.Options
	if opts != nil {
		nopts = opt.Options(*opts)
	}
	db, err := leveldb.OpenFile(path, &nopts)
	if err != nil {
		return nil, err
	}

	return &datastore{
		DB: db,
	}, nil
}

// Returns ErrInvalidType if value is not of type []byte.
//
// Note: using sync = false.
// see http://godoc.org/github.com/syndtr/goleveldb/leveldb/opt#WriteOptions
func (d *datastore) Put(key ds.Key, value interface{}) (err error) {
	val, ok := value.([]byte)
	if !ok {
		return ds.ErrInvalidType
	}
	return d.DB.Put(key.Bytes(), val, nil)
}

func (d *datastore) Get(key ds.Key) (value interface{}, err error) {
	val, err := d.DB.Get(key.Bytes(), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, ds.ErrNotFound
		}
		return nil, err
	}
	return val, nil
}

func (d *datastore) Has(key ds.Key) (exists bool, err error) {
	return d.DB.Has(key.Bytes(), nil)
}

func (d *datastore) Delete(key ds.Key) (err error) {
	// leveldb Delete will not return an error if the key doesn't
	// exist (see https://github.com/syndtr/goleveldb/issues/109),
	// so check that the key exists first and if not return an
	// error
	exists, err := d.DB.Has(key.Bytes(), nil)
	if !exists {
		return ds.ErrNotFound
	} else if err != nil {
		return err
	}
	return d.DB.Delete(key.Bytes(), nil)
}

func (d *datastore) Query(q dsq.Query) (dsq.Results, error) {

	var rnge *util.Range
	if q.Prefix != "" {
		rnge = util.BytesPrefix([]byte(q.Prefix))
	}

	r := &dsResults{}
	r.iter = d.DB.NewIterator(rnge, nil)
	r.q = q

	// advance iterator for offset
	if q.Offset > 0 {
		for j := 0; j < q.Offset; j++ {
			r.iter.Next()
		}
	}

	return r, nil
}

type dsResults struct {
	iter iterator.Iterator
	q    dsq.Query
	sent int
	ch   chan dsq.Result
}

func (r *dsResults) Close() error {
	r.iter.Release()
	r.iter = nil
	return nil
}

func (r *dsResults) Query() dsq.Query {
	return r.q
}

func (r *dsResults) Rest() ([]dsq.Entry, error) {
	var out []dsq.Entry
	for {
		v, ok := r.NextSync()
		if !ok {
			break
		}
		out = append(out, v.Entry)
	}
	return out, nil
}

func (r *dsResults) Next() <-chan dsq.Result {
	if r.ch != nil {
		return r.ch
	}

	r.ch = make(chan dsq.Result, 1)
	go func() {
		defer close(r.ch)
		for {
			v, ok := r.NextSync()
			if !ok {
				return
			}

			r.ch <- v
		}
	}()
	return r.ch
}

func (r *dsResults) NextSync() (dsq.Result, bool) {
	if !r.iter.Next() {
		return dsq.Result{}, false
	}

	// end early if we hit the limit
	if r.q.Limit > 0 && r.sent >= r.q.Limit {
		return dsq.Result{}, false
	}

	k := ds.NewKey(string(r.iter.Key())).String()
	e := dsq.Entry{Key: k}

	if !r.q.KeysOnly {
		buf := make([]byte, len(r.iter.Value()))
		copy(buf, r.iter.Value())
		e.Value = buf
	}

	r.sent++
	return dsq.Result{Entry: e}, true
}

func (r *dsResults) Process() goprocess.Process {
	return nil
}

// LevelDB needs to be closed.
func (d *datastore) Close() (err error) {
	return d.DB.Close()
}

func (d *datastore) IsThreadSafe() {}

type leveldbBatch struct {
	b  *leveldb.Batch
	db *leveldb.DB
}

func (d *datastore) Batch() (ds.Batch, error) {
	return &leveldbBatch{
		b:  new(leveldb.Batch),
		db: d.DB,
	}, nil
}

func (b *leveldbBatch) Put(key ds.Key, value interface{}) error {
	val, ok := value.([]byte)
	if !ok {
		return ds.ErrInvalidType
	}

	b.b.Put(key.Bytes(), val)
	return nil
}

func (b *leveldbBatch) Commit() error {
	return b.db.Write(b.b, nil)
}

func (b *leveldbBatch) Delete(key ds.Key) error {
	b.b.Delete(key.Bytes())
	return nil
}
