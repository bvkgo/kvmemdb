package kvmemdb

import (
	"context"
	"fmt"
	"testing"

	"golang.org/x/sync/errgroup"
)

func TestStaleReads(t *testing.T) {
	ctx := context.Background()

	var db DB
	tx1 := db.NewTx()
	if err := tx1.Set(ctx, "/foo", "foo"); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	tx2 := db.NewTx()
	tx3 := db.NewTx()
	if err := tx2.Set(ctx, "/foo", "FOO"); err != nil {
		t.Fatal(err)
	}
	if v, err := tx3.Get(ctx, "/foo"); err != nil {
		t.Fatal(err)
	} else if v != "foo" {
		t.Fatalf(`want "foo",  got %q`, v)
	}

	if err := tx2.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx3.Commit(ctx); err == nil {
		t.Fatalf("want non-nil error")
	} else {
		t.Log(err)
	}
}

func TestCreateConflict(t *testing.T) {
	ctx := context.Background()

	var db DB
	tx1 := db.NewTx()
	tx2 := db.NewTx()

	tx1.Set(ctx, "/foo", "foo")
	tx2.Set(ctx, "/foo", "foo")

	if err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(ctx); err == nil {
		t.Fatalf("want non-nil error")
	} else {
		t.Log(err)
	}
}

func TestCreateDeleteConflict(t *testing.T) {
	ctx := context.Background()

	var db DB
	tx1 := db.NewTx()
	tx2 := db.NewTx()

	tx1.Set(ctx, "/foo", "foo")
	tx2.Delete(ctx, "/foo")

	if err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteDeleteConflict(t *testing.T) {
	ctx := context.Background()

	var db DB
	tx1 := db.NewTx()
	tx2 := db.NewTx()

	tx1.Delete(ctx, "/foo")
	tx2.Delete(ctx, "/foo")

	if err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestNoConflicts(t *testing.T) {
	ctx := context.Background()

	var db DB
	tx1 := db.NewTx()
	tx2 := db.NewTx()

	tx1.Set(ctx, "/1", "one")
	tx2.Set(ctx, "/2", "two")

	if err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestMultiThreadedNonConflictingSets(t *testing.T) {
	ctx := context.Background()

	var db DB

	eg, ctx := errgroup.WithContext(ctx)

	npar := 10
	for i := 0; i < npar; i++ {
		offset := i

		eg.Go(func() error {
			tx := db.NewTx()

			for i := 0; i < 10; i++ {
				if err := tx.Set(ctx, fmt.Sprintf("/%d", i*npar+offset), fmt.Sprintf("%d", offset)); err != nil {
					return err
				}
			}

			if err := tx.Commit(ctx); err != nil {
				return err
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}
}
