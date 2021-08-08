package kvmemdb

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"golang.org/x/xerrors"
)

type IterateChecker struct {
	count int

	first, last string
}

func (c *IterateChecker) TestAscend(ctx context.Context, it *Iter) error {
	for k, _, err := it.GetNext(ctx); true; k, _, err = it.GetNext(ctx) {
		if err != nil {
			if !xerrors.Is(err, os.ErrNotExist) {
				return err
			}
			return nil
		}
		if len(k) == 0 {
			return xerrors.Errorf("key cannot be empty")
		}
		if len(c.last) > 0 {
			if k < c.last {
				return xerrors.Errorf("last key %q was larger than current %q", c.last, k)
			}
			if k == c.last {
				return xerrors.Errorf("last key %q was same as the current %q", c.last, k)
			}
		}
		if len(c.first) == 0 {
			c.first = k
		}
		c.count++
		c.last = k
	}
	return nil
}

func (c *IterateChecker) TestDescend(ctx context.Context, it *Iter) error {
	for k, _, err := it.GetNext(ctx); true; k, _, err = it.GetNext(ctx) {
		if err != nil {
			if !xerrors.Is(err, os.ErrNotExist) {
				return err
			}
			return nil
		}
		if len(k) == 0 {
			return xerrors.Errorf("key cannot be empty")
		}
		if len(c.last) > 0 {
			if k > c.last {
				return xerrors.Errorf("last key %q was smaller than current %q", c.last, k)
			}
			if k == c.last {
				return xerrors.Errorf("last key %q was same as the current %q", c.last, k)
			}
		}
		if len(c.first) == 0 {
			c.first = k
		}
		c.count++
		c.last = k
	}
	return nil
}

func RunAscendTest1(ctx context.Context, t *testing.T, newTx func() *Tx) error {
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
		if err := tx.Ascend(ctx, "", "", it); err != nil {
			t.Fatal(err)
		}
		if err := itcheck.TestAscend(ctx, it); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if itcheck.count != nkeys {
			t.Fatalf("wanted %d callbacks, got %d", nkeys, itcheck.count)
		}
		if itcheck.first != smallest {
			t.Fatalf("wanted %q as the first, got %q", smallest, itcheck.first)
		}
		if itcheck.last != largest {
			t.Fatalf("wanted %q as the last, got %q", largest, itcheck.last)
		}
	}

	// Iterate till the largest key with one of i or j as the empty string.
	{
		r := rand.Intn(nkeys)
		x := fmt.Sprintf("%03d", r)

		tx := newTx()
		it := &Iter{}
		var itcheck IterateChecker
		if err := tx.Ascend(ctx, x, "", it); err != nil {
			return nil
		}
		if err := itcheck.TestAscend(ctx, it); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if itcheck.first != x {
			t.Fatalf("wanted %s as the first, got %s", x, itcheck.first)
		}
		if itcheck.last != largest {
			t.Fatalf("wanted %s as the last, got %s", largest, itcheck.last)
		}
		if itcheck.count != nkeys-r {
			t.Fatalf("wanted %d callbacks, got %d", nkeys-r, itcheck.count)
		}
	}
	{
		r := rand.Intn(nkeys)
		x := fmt.Sprintf("%03d", r)

		tx := newTx()
		it := &Iter{}
		var itcheck IterateChecker
		if err := tx.Ascend(ctx, "", x, it); err != nil {
			return nil
		}
		if err := itcheck.TestAscend(ctx, it); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if itcheck.first != x {
			t.Fatalf("wanted %s as the first, got %s", x, itcheck.first)
		}
		if itcheck.last != largest {
			t.Fatalf("wanted %s as the last, got %s", largest, itcheck.last)
		}
		if itcheck.count != nkeys-r {
			t.Fatalf("wanted %d callbacks, got %d", nkeys-r, itcheck.count)
		}
	}

	// Iterate randomly picked range.
	{
		f := rand.Intn(nkeys)
		l := rand.Intn(nkeys)
		if f > l {
			f, l = l, f
		}
		x := fmt.Sprintf("%03d", f)
		y := fmt.Sprintf("%03d", l)
		min, max, count := x, fmt.Sprintf("%03d", l-1), l-f

		tx := newTx()
		it := &Iter{}
		var itcheck IterateChecker
		if err := tx.Ascend(ctx, x, y, it); err != nil {
			t.Fatal(err)
		}
		if err := itcheck.TestAscend(ctx, it); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if itcheck.count != count {
			t.Fatalf("wanted %d callbacks, got %d", count, itcheck.count)
		}
		if count > 0 {
			if itcheck.first != min {
				t.Fatalf("wanted %s as the first, got %s", min, itcheck.first)
			}
			if itcheck.last != max {
				t.Fatalf("wanted %s as the last, got %s", max, itcheck.last)
			}
		}
	}

	return nil
}

func TestAscending(t *testing.T) {
	ctx := context.Background()

	var db DB
	if err := RunAscendTest1(ctx, t, db.NewTx); err != nil {
		t.Fatal(err)
	}
}
