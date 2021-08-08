package kvmemdb

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
)

func RunDescendTest1(ctx context.Context, t *testing.T, newTx func() *Tx) error {
	nkeys := 1000
	tx1 := newTx()
	for i := 0; i < nkeys; i++ {
		s := fmt.Sprintf("%03d", i)
		if err := tx1.Set(ctx, s, s); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	smallest := fmt.Sprintf("%03d", 0)
	largest := fmt.Sprintf("%03d", nkeys-1)

	// Iterate all keys in ascending order.
	{
		var itcheck IterateChecker

		tx := newTx()
		it := &Iter{}
		if err := tx.Descend(ctx, "", "", it); err != nil {
			t.Fatal(err)
		}
		if err := itcheck.TestDescend(ctx, it); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if itcheck.count != nkeys {
			t.Fatalf("wanted %d callbacks, got %d", nkeys, itcheck.count)
		}
		if itcheck.first != largest {
			t.Fatalf("wanted %q as the first, got %q", largest, itcheck.first)
		}
		if itcheck.last != smallest {
			t.Fatalf("wanted %q as the last, got %q", smallest, itcheck.last)
		}
	}

	// Iterate till the smallest key with one of i or j as the empty string.
	{
		r := rand.Intn(nkeys)
		x := fmt.Sprintf("%03d", r)

		tx := newTx()
		it := &Iter{}
		var itcheck IterateChecker
		if err := tx.Descend(ctx, x, "", it); err != nil {
			t.Fatal(err)
		}
		if err := itcheck.TestDescend(ctx, it); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if itcheck.first != x {
			t.Fatalf("wanted %s as the first, got %s", x, itcheck.first)
		}
		if itcheck.last != smallest {
			t.Fatalf("wanted %s as the last, got %s", smallest, itcheck.last)
		}
		if itcheck.count != r+1 {
			t.Fatalf("wanted %d callbacks, got %d", r+1, itcheck.count)
		}
	}
	{
		r := rand.Intn(nkeys)
		x := fmt.Sprintf("%03d", r)

		tx := newTx()
		it := &Iter{}
		var itcheck IterateChecker
		if err := tx.Descend(ctx, "", x, it); err != nil {
			t.Fatal(err)
		}
		if err := itcheck.TestDescend(ctx, it); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if itcheck.first != x {
			t.Fatalf("wanted %s as the first, got %s", x, itcheck.first)
		}
		if itcheck.last != smallest {
			t.Fatalf("wanted %s as the last, got %s", smallest, itcheck.last)
		}
		if itcheck.count != r+1 {
			t.Fatalf("wanted %d callbacks, got %d", r+1, itcheck.count)
		}
	}

	// Iterate randomly picked range.
	{
		f := rand.Intn(nkeys)
		l := rand.Intn(nkeys)
		if f < l {
			f, l = l, f
		}
		x := fmt.Sprintf("%03d", f)
		y := fmt.Sprintf("%03d", l)
		min, max, count := fmt.Sprintf("%03d", l+1), x, f-l

		tx := newTx()
		it := &Iter{}
		var itcheck IterateChecker
		if err := tx.Descend(ctx, x, y, it); err != nil {
			t.Fatal(err)
		}
		if err := itcheck.TestDescend(ctx, it); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if itcheck.count != count {
			t.Fatalf("wanted %d callbacks, got %d", count, itcheck.count)
		}
		if count > 0 {
			if itcheck.first != max {
				t.Fatalf("wanted %s as the first, got %s", max, itcheck.first)
			}
			if itcheck.last != min {
				t.Fatalf("wanted %s as the last, got %s", min, itcheck.last)
			}
		}
	}

	return nil
}

func TestDescending(t *testing.T) {
	ctx := context.Background()

	var db DB
	if err := RunDescendTest1(ctx, t, db.NewTx); err != nil {
		t.Fatal(err)
	}
}
