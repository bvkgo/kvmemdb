package kvmemdb

import (
	"context"
	"database/sql"
	"os"
	"sort"
	"time"
)

type Tx struct {
	db *DB

	snap kvMap

	accessed kvMap

	filter func(string) bool
}

func (t *Tx) checkFilter(k string) bool {
	if t.filter == nil {
		return true
	}
	return t.filter(k)
}

// Commit applies all transaction updates atomically to the database.
func (t *Tx) Commit(ctx context.Context) error {
	if t.db == nil {
		return sql.ErrTxDone
	}

	db := t.db
	t.db = nil
	return db.tryCommit(t)
}

// Rollback drops the transaction.
func (t *Tx) Rollback(ctx context.Context) error {
	if t.db == nil {
		return sql.ErrTxDone
	}
	t.db = nil
	return nil
}

// Get retrieves the value for a key.
func (t *Tx) Get(ctx context.Context, key string) (string, error) {
	if !t.checkFilter(key) {
		return "", os.ErrInvalid
	}

	kv, ok := t.accessed.Get(key)
	if !ok {
		kv, ok = t.snap.Get(key)
	}

	if !ok || kv.isDeleted() {
		return "", os.ErrNotExist
	}

	t.accessed = t.accessed.Set(kv)
	return kv.value, nil
}

// Set updates the value for a key or inserts a new key-value pair if key
// doesn't already exist.
func (t *Tx) Set(ctx context.Context, key, value string) error {
	if t.db == nil {
		return sql.ErrTxDone
	}
	if !t.db.checkKey(key) {
		return os.ErrInvalid
	}
	if !t.checkFilter(key) {
		return os.ErrInvalid
	}

	kv, ok := t.accessed.Get(key)
	if !ok {
		kv, ok = t.snap.Get(key)
	}

	if !ok || kv.isDeleted() {
		t.accessed = t.accessed.Set(newPair(key, value))
		return nil
	}

	kv.mtime = time.Now()
	kv.value = value
	t.accessed = t.accessed.Set(kv)
	return nil
}

// Delete removes a key-value pair. Key is NOT considered accessed by the
// transaction when it doesn't exist, which is consistent with the behavior of
// other functions.
func (t *Tx) Delete(ctx context.Context, key string) error {
	if t.db == nil {
		return sql.ErrTxDone
	}
	if !t.db.checkKey(key) {
		return os.ErrInvalid
	}
	if !t.checkFilter(key) {
		return os.ErrInvalid
	}

	kv, ok := t.accessed.Get(key)
	if !ok {
		kv, ok = t.snap.Get(key)
	}

	if !ok || kv.isDeleted() {
		return os.ErrNotExist
	}

	kv.mtime = time.Time{}
	t.accessed = t.accessed.Set(kv)
	return nil
}

func (t *Tx) allLive(ordered bool) [][2]string {
	var kvs [][2]string
	for _, kv := range t.accessed {
		if !kv.isDeleted() && t.checkFilter(kv.key) {
			kvs = append(kvs, [2]string{kv.key, kv.value})
		}
	}
	for _, kv := range t.snap {
		if _, ok := t.accessed.Get(kv.key); !ok {
			if !kv.isDeleted() && t.checkFilter(kv.key) {
				kvs = append(kvs, [2]string{kv.key, kv.value})
			}
		}
	}
	if ordered {
		sort.Slice(kvs, func(i, j int) bool {
			return kvs[i][0] < kvs[j][0]
		})
	}
	return kvs
}

func (t *Tx) touch(k string) bool {
	if _, ok := t.accessed.Index(k); !ok {
		if kv, ok := t.snap.Get(k); ok {
			t.accessed = t.accessed.Set(kv)
			return true
		}
	}
	return false
}

// Scan calls the user-defined callback function for every key-value pair in no
// particular order.
func (t *Tx) Scan(ctx context.Context, cb func(context.Context, string, string) error) error {
	kvs := t.allLive(false /* sort */)
	for _, kv := range kvs {
		t.touch(kv[0])
		if err := cb(ctx, kv[0], kv[1]); err != nil {
			return err
		}
	}
	return nil
}

// Ascend calls the user-defined callback function for the selected range in
// ascending order.
func (t *Tx) Ascend(ctx context.Context, begin, end string, cb func(context.Context, string, string) error) error {
	if begin > end || (begin == end && begin != "") {
		return nil
	}

	kvs := t.allLive(true /* sort */)

	i := 0
	if len(begin) > 0 {
		i = sort.Search(len(kvs), func(x int) bool {
			return kvs[x][0] >= begin
		})
	}

	j := len(kvs)
	if len(end) > 0 {
		j = sort.Search(len(kvs), func(x int) bool {
			return kvs[x][0] >= end
		})
	}

	for ; i < j; i++ {
		t.touch(kvs[i][0])
		if err := cb(ctx, kvs[i][0], kvs[i][1]); err != nil {
			return err
		}
	}
	return nil
}

// Descend calls the user-defined callback function for the selected range in
// descending order.
func (t *Tx) Descend(ctx context.Context, begin, end string, cb func(context.Context, string, string) error) error {
	if begin < end || (begin == end && begin != "") {
		return nil
	}

	kvs := t.allLive(true /* sort */)

	i := len(kvs) - 1
	if len(begin) > 0 {
		i = sort.Search(len(kvs), func(x int) bool {
			return kvs[x][0] >= begin
		})
	}

	j := -1
	if len(end) > 0 {
		j = sort.Search(len(kvs), func(x int) bool {
			return kvs[x][0] >= end
		})
	}

	for ; i > j; i-- {
		t.touch(kvs[i][0])
		if err := cb(ctx, kvs[i][0], kvs[i][1]); err != nil {
			return err
		}
	}
	return nil
}
