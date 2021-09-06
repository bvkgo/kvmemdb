package kvmemdb

import (
	"context"
	"testing"

	"github.com/bvkgo/kv"
	"github.com/bvkgo/kvtests"
)

func TestIsolation1(t *testing.T) {
	ctx := context.Background()

	db := New(nil)
	opts := &kvtests.Options{
		NewTx: func(ctx context.Context) (kv.Transaction, error) {
			return db.NewTx(), nil
		},
		NewIt: func(ctx context.Context) (kv.Iterator, error) {
			return new(Iter), nil
		},
	}

	kvtests.RunAllIsolationTests(t, ctx, opts)
}
