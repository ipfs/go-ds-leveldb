package leveldb

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"testing"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	dstest "github.com/ipfs/go-datastore/test"
)

var testcases = map[string]string{
	"/a":     "a",
	"/a/b":   "ab",
	"/a/b/c": "abc",
	"/a/b/d": "a/b/d",
	"/a/c":   "ac",
	"/a/d":   "ad",
	"/e":     "e",
	"/f":     "f",
}

// newDSPath returns a new memory or disk backed datastore, depending on
// whether path is specified.. At then end of the test the returned datastore
// is closed and its directory is removed.
func newDSPath(t *testing.T, path string) *Datastore {
	t.Helper()
	d, err := NewDatastore(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	})
	return d
}

// newDS returns a disk-backed datastore.
func newDS(t *testing.T) *Datastore {
	t.Helper()
	return newDSPath(t, t.TempDir())
}

// newDSMem returns an in-memory datastore.
func newDSMem(t *testing.T) *Datastore {
	t.Helper()
	return newDSPath(t, "")
}

func addTestCases(t *testing.T, d *Datastore, testcases map[string]string) {
	ctx := t.Context()
	for k, v := range testcases {
		dsk := ds.NewKey(k)
		if err := d.Put(ctx, dsk, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	for k, v := range testcases {
		dsk := ds.NewKey(k)
		v2, err := d.Get(ctx, dsk)
		if err != nil {
			t.Fatal(err)
		}
		if string(v2) != v {
			t.Errorf("%s values differ: %s != %s", k, v, v2)
		}
	}

}

func testQuery(t *testing.T, d *Datastore) {
	ctx := t.Context()
	addTestCases(t, d, testcases)

	rs, err := d.Query(ctx, dsq.Query{Prefix: "/a/"})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{
		"/a/b",
		"/a/b/c",
		"/a/b/d",
		"/a/c",
		"/a/d",
	}, rs)

	// test offset and limit

	rs, err = d.Query(ctx, dsq.Query{Prefix: "/a/", Offset: 2, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{
		"/a/b/d",
		"/a/c",
	}, rs)

	// test order

	rs, err = d.Query(ctx, dsq.Query{Orders: []dsq.Order{dsq.OrderByKey{}}})
	if err != nil {
		t.Fatal(err)
	}

	keys := make([]string, 0, len(testcases))
	for k := range testcases {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	expectOrderedMatches(t, keys, rs)

	rs, err = d.Query(ctx, dsq.Query{Orders: []dsq.Order{dsq.OrderByKeyDescending{}}})
	if err != nil {
		t.Fatal(err)
	}

	// reverse
	for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
		keys[i], keys[j] = keys[j], keys[i]
	}

	expectOrderedMatches(t, keys, rs)
}

func TestQuery(t *testing.T) {
	d := newDS(t)
	testQuery(t, d)
}
func TestQueryMem(t *testing.T) {
	d := newDSMem(t)
	testQuery(t, d)
}

func TestQueryRespectsProcess(t *testing.T) {
	d := newDS(t)
	addTestCases(t, d, testcases)
}

func TestCloseRace(t *testing.T) {
	d, err := NewDatastore(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	for n := range 100 {
		d.Put(ctx, ds.NewKey(fmt.Sprintf("%d", n)), fmt.Appendf(nil, "test%d", n))
	}

	tx, _ := d.NewTransaction(ctx, false)
	tx.Put(ctx, ds.NewKey("txnversion"), []byte("bump"))

	closeCh := make(chan any)

	go func() {
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		closeCh <- nil
	}()
	for k := range testcases {
		tx.Get(ctx, ds.NewKey(k))
	}
	tx.Commit(ctx)
	<-closeCh
}

func TestCloseSafety(t *testing.T) {
	d, err := NewDatastore(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	addTestCases(t, d, testcases)

	ctx := t.Context()
	tx, _ := d.NewTransaction(ctx, false)
	err = tx.Put(ctx, ds.NewKey("test"), []byte("test"))
	if err != nil {
		t.Error("Failed to put in a txn.")
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	err = tx.Commit(ctx)
	if err == nil {
		t.Error("committing after close should fail.")
	}
}

func TestQueryRespectsProcessMem(t *testing.T) {
	d := newDSMem(t)
	addTestCases(t, d, testcases)
}

func expectMatches(t *testing.T, expect []string, actualR dsq.Results) {
	t.Helper()
	actual, err := actualR.Rest()
	if err != nil {
		t.Error(err)
	}

	if len(actual) != len(expect) {
		t.Error("not enough", expect, actual)
	}
	for _, k := range expect {
		found := false
		for _, e := range actual {
			if e.Key == k {
				found = true
			}
		}
		if !found {
			t.Error(k, "not found")
		}
	}
}

func expectOrderedMatches(t *testing.T, expect []string, actualR dsq.Results) {
	t.Helper()
	actual, err := actualR.Rest()
	if err != nil {
		t.Error(err)
	}

	if len(actual) != len(expect) {
		t.Error("not enough", expect, actual)
	}
	for i := range expect {
		if expect[i] != actual[i].Key {
			t.Errorf("expected %q, got %q", expect[i], actual[i].Key)
		}
	}
}

func testBatching(t *testing.T, d *Datastore) {
	ctx := t.Context()
	b, err := d.Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testcases {
		err := b.Put(ctx, ds.NewKey(k), []byte(v))
		if err != nil {
			t.Fatal(err)
		}
	}

	err = b.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testcases {
		val, err := d.Get(ctx, ds.NewKey(k))
		if err != nil {
			t.Fatal(err)
		}

		if v != string(val) {
			t.Fatal("got wrong data!")
		}
	}
}

func TestBatching(t *testing.T) {
	d := newDS(t)
	testBatching(t, d)
}

func TestBatchingMem(t *testing.T) {
	d := newDSMem(t)
	testBatching(t, d)
}

func TestDiskUsage(t *testing.T) {
	tempDir := t.TempDir()
	d, err := NewDatastore(tempDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	addTestCases(t, d, testcases)
	ctx := t.Context()
	du, err := d.DiskUsage(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if du == 0 {
		t.Fatal("expected some disk usage")
	}

	k := ds.NewKey("more")
	err = d.Put(ctx, k, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}

	du2, err := d.DiskUsage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if du2 <= du {
		t.Fatal("size should have increased")
	}

	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	err = os.RemoveAll(tempDir)
	if err != nil {
		t.Fatal(err)
	}

	// This should fail
	_, err = d.DiskUsage(ctx)
	if err == nil {
		t.Fatal("DiskUsage should fail when we cannot walk path")
	}
}

func TestDiskUsageInMem(t *testing.T) {
	d := newDSMem(t)
	du, _ := d.DiskUsage(t.Context())
	if du != 0 {
		t.Fatal("inmem dbs have 0 disk usage")
	}
}

func TestTransactionCommit(t *testing.T) {
	key := ds.NewKey("/test/key1")

	ctx := t.Context()
	d := newDS(t)

	txn, err := d.NewTransaction(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Discard(ctx)

	if err := txn.Put(ctx, key, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if val, err := d.Get(ctx, key); err != ds.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got err: %v, value: %v", err, val)
	}
	if err := txn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if val, err := d.Get(ctx, key); err != nil || !bytes.Equal(val, []byte("hello")) {
		t.Fatalf("expected entry present after commit, got err: %v, value: %v", err, val)
	}
}

func TestTransactionDiscard(t *testing.T) {
	key := ds.NewKey("/test/key1")

	ctx := t.Context()
	d := newDS(t)

	txn, err := d.NewTransaction(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Discard(ctx)

	if err := txn.Put(ctx, key, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if val, err := d.Get(ctx, key); err != ds.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got err: %v, value: %v", err, val)
	}
	if txn.Discard(ctx); err != nil {
		t.Fatal(err)
	}
	if val, err := d.Get(ctx, key); err != ds.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got err: %v, value: %v", err, val)
	}
}

func TestTransactionManyOperations(t *testing.T) {
	keys := []ds.Key{ds.NewKey("/test/key1"), ds.NewKey("/test/key2"), ds.NewKey("/test/key3"), ds.NewKey("/test/key4"), ds.NewKey("/test/key5")}

	ctx := t.Context()
	d := newDS(t)

	txn, err := d.NewTransaction(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Discard(ctx)

	// Insert all entries.
	for i := range 5 {
		if err := txn.Put(ctx, keys[i], fmt.Appendf(nil, "hello%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	// Remove the third entry.
	if err := txn.Delete(ctx, keys[2]); err != nil {
		t.Fatal(err)
	}

	// Check existences.
	if has, err := txn.Has(ctx, keys[1]); err != nil || !has {
		t.Fatalf("expected key[1] to be present, err: %v, has: %v", err, has)
	}
	if has, err := txn.Has(ctx, keys[2]); err != nil || has {
		t.Fatalf("expected key[2] to be absent, err: %v, has: %v", err, has)
	}

	var res dsq.Results
	if res, err = txn.Query(ctx, dsq.Query{Prefix: "/test"}); err != nil {
		t.Fatalf("query failed, err: %v", err)
	}
	if entries, err := res.Rest(); err != nil || len(entries) != 4 {
		t.Fatalf("query failed or contained unexpected number of entries, err: %v, results: %v", err, entries)
	}

	txn.Discard(ctx)
}

func TestSuite(t *testing.T) {
	d := newDSMem(t)
	dstest.SubtestAll(t, d)
}
