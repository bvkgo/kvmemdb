package kvmemdb

import (
	"context"
	"testing"

	"github.com/bvkgo/kv"
	"github.com/bvkgo/kvtests"
)

func TestAscending1(t *testing.T) {
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
	if err := kvtests.RunAscendTest1(ctx, opts); err != nil {
		t.Fatal(err)
	}
}

func TestDescending1(t *testing.T) {
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
	if err := kvtests.RunDescendTest1(ctx, opts); err != nil {
		t.Fatal(err)
	}
}
