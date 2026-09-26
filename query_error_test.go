package leveldb

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func TestQueryClosedDatastore(t *testing.T) {
	d, err := NewDatastore("", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	for _, order := range []dsq.Order{dsq.OrderByKey{}, dsq.OrderByKeyDescending{}, dsq.OrderByValue{}} {
		t.Run(fmt.Sprintf("%T", order), func(t *testing.T) {
			r, err := d.Query(t.Context(), dsq.Query{Orders: []dsq.Order{order}})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if _, err := r.Rest(); !errors.Is(err, leveldb.ErrClosed) {
				t.Fatalf("expected ErrClosed, got %v", err)
			}
			if result, ok := r.NextSync(); ok {
				t.Fatalf("expected end after error, got %+v", result)
			}
		})
	}
}

// Use the real LevelDB table iterator, failing only the backing ReadAt calls
// after the first result. Small, uncached blocks force another storage read.
func TestQueryTableReadError(t *testing.T) {
	for _, descending := range []bool{false, true} {
		for _, fail := range []bool{false, true} {
			for _, channel := range []bool{false, true} {
				t.Run(fmt.Sprintf("descending=%v/fail=%v/channel=%v", descending, fail, channel), func(t *testing.T) {
					readErr := errors.New("table read failed")
					s := &queryErrorStorage{Storage: storage.NewMemStorage(), err: readErr}
					db, err := leveldb.Open(s, &opt.Options{
						BlockSize: 128, BlockCacheCapacity: -1,
						Compression: opt.NoCompression, Strict: opt.StrictAll,
					})
					if err != nil {
						t.Fatal(err)
					}
					d := &Datastore{DB: db, accessor: &accessor{ldb: db, closeLk: new(sync.RWMutex)}}
					t.Cleanup(func() {
						if err := d.Close(); err != nil {
							t.Error(err)
						}
					})
					const count = 100
					for i := range count {
						if err := d.Put(t.Context(), ds.NewKey(fmt.Sprintf("/records/%03d", i)), make([]byte, 100)); err != nil {
							t.Fatal(err)
						}
					}
					if err := db.CompactRange(util.Range{}); err != nil {
						t.Fatal(err)
					}
					var order dsq.Order = dsq.OrderByKey{}
					if descending {
						order = dsq.OrderByKeyDescending{}
					}
					r, err := d.Query(t.Context(), dsq.Query{Prefix: "/records", Orders: []dsq.Order{order}})
					if err != nil {
						t.Fatal(err)
					}
					defer r.Close()
					first, ok := r.NextSync()
					if !ok || first.Error != nil {
						t.Fatalf("expected first entry, got %+v, %v", first, ok)
					}
					s.fail.Store(fail)
					entries, errs := 1, 0
					check := func(result dsq.Result) {
						if result.Error != nil {
							errs++
							if !errors.Is(result.Error, readErr) {
								t.Fatalf("expected storage error, got %v", result.Error)
							}
						} else {
							entries++
						}
						if errs > 1 || entries > count {
							t.Fatalf("iterator did not stop: %d entries, %d errors", entries, errs)
						}
					}
					if channel {
						for result := range r.Next() {
							check(result)
						}
					} else {
						for result, ok := r.NextSync(); ok; result, ok = r.NextSync() {
							check(result)
						}
					}
					if fail && (errs != 1 || entries >= count) {
						t.Fatalf("expected partial results and one error: %d entries, %d errors", entries, errs)
					}
					if !fail && (errs != 0 || entries != count) {
						t.Fatalf("expected complete results: %d entries, %d errors", entries, errs)
					}
					if result, ok := r.NextSync(); ok {
						t.Fatalf("expected exhausted iterator, got %+v", result)
					}
				})
			}
		}
	}
}

type queryErrorStorage struct {
	storage.Storage
	fail atomic.Bool
	err  error
}

func (s *queryErrorStorage) Open(fd storage.FileDesc) (storage.Reader, error) {
	r, err := s.Storage.Open(fd)
	if err != nil {
		return nil, err
	}
	if fd.Type == storage.TypeTable {
		return &queryErrorReader{Reader: r, store: s}, nil
	}
	return r, nil
}

type queryErrorReader struct {
	storage.Reader
	store *queryErrorStorage
}

func (r *queryErrorReader) ReadAt(p []byte, off int64) (int, error) {
	if r.store.fail.Load() {
		return 0, r.store.err
	}
	return r.Reader.ReadAt(p, off)
}
