package kvmemdb

import (
	"context"
	"testing"

	"golang.org/x/xerrors"
)

func insertDigits(ctx context.Context, t *testing.T, db *DB) {
	tx := db.NewTx()
	if err := tx.Set(ctx, "0", "0"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set(ctx, "1", "1"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set(ctx, "2", "2"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set(ctx, "3", "3"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set(ctx, "4", "4"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set(ctx, "5", "5"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set(ctx, "6", "6"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set(ctx, "7", "7"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set(ctx, "8", "8"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set(ctx, "9", "9"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestIterateRange(t *testing.T) {
	ctx := context.Background()

	var db DB
	insertDigits(ctx, t, &db)

	tx1 := db.NewTx()
	defer tx1.Rollback(ctx)

	cb := func(_ context.Context, k, v string) error {
		if k < "2" {
			return xerrors.Errorf("%s is unexpected", k)
		}
		if k >= "8" {
			return xerrors.Errorf("%s is unexpected", k)
		}
		return nil
	}
	if err := tx1.Ascend(ctx, "2", "8", cb); err != nil {
		t.Fatal(err)
	}
}

func TestIterateMissingRange(t *testing.T) {
	ctx := context.Background()

	var db DB
	insertDigits(ctx, t, &db)

	tx1 := db.NewTx()
	defer tx1.Rollback(ctx)

	cb := func(_ context.Context, k, v string) error {
		if k < "2" {
			return xerrors.Errorf("%s is unexpected", k)
		}
		if k >= "8" {
			return xerrors.Errorf("%s is unexpected", k)
		}
		return nil
	}
	if err := tx1.Ascend(ctx, "10", "8", cb); err != nil {
		t.Fatal(err)
	}
}

func TestIterateWithDeletions(t *testing.T) {
	ctx := context.Background()

	var db DB
	insertDigits(ctx, t, &db)

	tx1 := db.NewTx()
	defer tx1.Rollback(ctx)

	if err := tx1.Delete(ctx, "5"); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Set(ctx, "55", "55"); err != nil {
		t.Fatal(err)
	}

	cb := func(_ context.Context, k, v string) error {
		if k == "5" {
			return xerrors.Errorf("5 was deleted")
		}
		return nil
	}
	if err := tx1.Ascend(ctx, "", "", cb); err != nil {
		t.Fatal(err)
	}
}

func TestIterateWithUpdates(t *testing.T) {
	ctx := context.Background()

	var db DB
	insertDigits(ctx, t, &db)

	tx1 := db.NewTx()
	defer tx1.Rollback(ctx)

	if err := tx1.Set(ctx, "5", "55"); err != nil {
		t.Fatal(err)
	}

	cb := func(_ context.Context, k, v string) error {
		if k == "5" && v != "55" {
			return xerrors.Errorf("key 5 must have value 55")
		}
		return nil
	}
	if err := tx1.Ascend(ctx, "", "", cb); err != nil {
		t.Fatal(err)
	}
}
