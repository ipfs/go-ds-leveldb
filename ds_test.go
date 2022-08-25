package leveldb

import (
	"bytes"
	"context"
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

var bg = context.Background()

// returns datastore, and a function to call on exit.
// (this garbage collects). So:
//
//	d, close := newDS(t)
//	defer close()
func newDS(t *testing.T) (*Datastore, func()) {
	path, err := os.MkdirTemp("", "testing_leveldb_")
	if err != nil {
		t.Fatal(err)
	}

	d, err := NewDatastore(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	return d, func() {
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(path); err != nil {
			t.Fatal(err)
		}
	}
}

// newDSMem returns an in-memory datastore.
func newDSMem(t *testing.T) *Datastore {
	d, err := NewDatastore("", nil)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func addTestCases(t *testing.T, d *Datastore, testcases map[string]string) {
	for k, v := range testcases {
		dsk := ds.NewKey(k)
		if err := d.Put(bg, dsk, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	for k, v := range testcases {
		dsk := ds.NewKey(k)
		v2, err := d.Get(bg, dsk)
		if err != nil {
			t.Fatal(err)
		}
		if string(v2) != v {
			t.Errorf("%s values differ: %s != %s", k, v, v2)
		}
	}

}

func testQuery(t *testing.T, d *Datastore) {
	addTestCases(t, d, testcases)

	rs, err := d.Query(bg, dsq.Query{Prefix: "/a/"})
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

	rs, err = d.Query(bg, dsq.Query{Prefix: "/a/", Offset: 2, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{
		"/a/b/d",
		"/a/c",
	}, rs)

	// test order

	rs, err = d.Query(bg, dsq.Query{Orders: []dsq.Order{dsq.OrderByKey{}}})
	if err != nil {
		t.Fatal(err)
	}

	keys := make([]string, 0, len(testcases))
	for k := range testcases {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	expectOrderedMatches(t, keys, rs)

	rs, err = d.Query(bg, dsq.Query{Orders: []dsq.Order{dsq.OrderByKeyDescending{}}})
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
	d, close := newDS(t)
	defer close()
	testQuery(t, d)
}
func TestQueryMem(t *testing.T) {
	d := newDSMem(t)
	testQuery(t, d)
}

func TestQueryRespectsProcess(t *testing.T) {
	d, close := newDS(t)
	defer close()
	addTestCases(t, d, testcases)
}

func TestCloseRace(t *testing.T) {
	d, close := newDS(t)
	for n := 0; n < 100; n++ {
		d.Put(bg, ds.NewKey(fmt.Sprintf("%d", n)), []byte(fmt.Sprintf("test%d", n)))
	}

	tx, _ := d.NewTransaction(bg, false)
	tx.Put(bg, ds.NewKey("txnversion"), []byte("bump"))

	closeCh := make(chan interface{})

	go func() {
		close()
		closeCh <- nil
	}()
	for k := range testcases {
		tx.Get(bg, ds.NewKey(k))
	}
	tx.Commit(bg)
	<-closeCh
}

func TestCloseSafety(t *testing.T) {
	d, close := newDS(t)
	addTestCases(t, d, testcases)

	tx, _ := d.NewTransaction(bg, false)
	err := tx.Put(bg, ds.NewKey("test"), []byte("test"))
	if err != nil {
		t.Error("Failed to put in a txn.")
	}
	close()
	err = tx.Commit(bg)
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
	b, err := d.Batch(bg)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testcases {
		err := b.Put(bg, ds.NewKey(k), []byte(v))
		if err != nil {
			t.Fatal(err)
		}
	}

	err = b.Commit(bg)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testcases {
		val, err := d.Get(bg, ds.NewKey(k))
		if err != nil {
			t.Fatal(err)
		}

		if v != string(val) {
			t.Fatal("got wrong data!")
		}
	}
}

func TestBatching(t *testing.T) {
	d, done := newDS(t)
	defer done()
	testBatching(t, d)
}

func TestBatchingMem(t *testing.T) {
	d := newDSMem(t)
	testBatching(t, d)
}

func TestDiskUsage(t *testing.T) {
	d, done := newDS(t)
	addTestCases(t, d, testcases)
	du, err := d.DiskUsage(bg)
	if err != nil {
		t.Fatal(err)
	}

	if du == 0 {
		t.Fatal("expected some disk usage")
	}

	k := ds.NewKey("more")
	err = d.Put(bg, k, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}

	du2, err := d.DiskUsage(bg)
	if err != nil {
		t.Fatal(err)
	}
	if du2 <= du {
		t.Fatal("size should have increased")
	}

	done()

	// This should fail
	_, err = d.DiskUsage(bg)
	if err == nil {
		t.Fatal("DiskUsage should fail when we cannot walk path")
	}
}

func TestDiskUsageInMem(t *testing.T) {
	d := newDSMem(t)
	du, _ := d.DiskUsage(bg)
	if du != 0 {
		t.Fatal("inmem dbs have 0 disk usage")
	}
}

func TestTransactionCommit(t *testing.T) {
	key := ds.NewKey("/test/key1")

	d, done := newDS(t)
	defer done()

	txn, err := d.NewTransaction(bg, false)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Discard(bg)

	if err := txn.Put(bg, key, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if val, err := d.Get(bg, key); err != ds.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got err: %v, value: %v", err, val)
	}
	if err := txn.Commit(bg); err != nil {
		t.Fatal(err)
	}
	if val, err := d.Get(bg, key); err != nil || !bytes.Equal(val, []byte("hello")) {
		t.Fatalf("expected entry present after commit, got err: %v, value: %v", err, val)
	}
}

func TestTransactionDiscard(t *testing.T) {
	key := ds.NewKey("/test/key1")

	d, done := newDS(t)
	defer done()

	txn, err := d.NewTransaction(bg, false)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Discard(bg)

	if err := txn.Put(bg, key, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if val, err := d.Get(bg, key); err != ds.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got err: %v, value: %v", err, val)
	}
	if txn.Discard(bg); err != nil {
		t.Fatal(err)
	}
	if val, err := d.Get(bg, key); err != ds.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got err: %v, value: %v", err, val)
	}
}

func TestTransactionManyOperations(t *testing.T) {
	keys := []ds.Key{ds.NewKey("/test/key1"), ds.NewKey("/test/key2"), ds.NewKey("/test/key3"), ds.NewKey("/test/key4"), ds.NewKey("/test/key5")}

	d, done := newDS(t)
	defer done()

	txn, err := d.NewTransaction(bg, false)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Discard(bg)

	// Insert all entries.
	for i := 0; i < 5; i++ {
		if err := txn.Put(bg, keys[i], []byte(fmt.Sprintf("hello%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// Remove the third entry.
	if err := txn.Delete(bg, keys[2]); err != nil {
		t.Fatal(err)
	}

	// Check existences.
	if has, err := txn.Has(bg, keys[1]); err != nil || !has {
		t.Fatalf("expected key[1] to be present, err: %v, has: %v", err, has)
	}
	if has, err := txn.Has(bg, keys[2]); err != nil || has {
		t.Fatalf("expected key[2] to be absent, err: %v, has: %v", err, has)
	}

	var res dsq.Results
	if res, err = txn.Query(bg, dsq.Query{Prefix: "/test"}); err != nil {
		t.Fatalf("query failed, err: %v", err)
	}
	if entries, err := res.Rest(); err != nil || len(entries) != 4 {
		t.Fatalf("query failed or contained unexpected number of entries, err: %v, results: %v", err, entries)
	}

	txn.Discard(bg)
}

func TestSuite(t *testing.T) {
	d := newDSMem(t)
	defer d.Close()
	dstest.SubtestAll(t, d)
}
