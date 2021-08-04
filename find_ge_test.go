package kvmemdb

import (
	"context"
	"os"
	"testing"

	"golang.org/x/xerrors"
)

func TestFindGE0(t *testing.T) {
	ctx := context.Background()

	var db DB
	tx0 := db.NewTx()
	tx0.Set(ctx, "1", "1")
	tx0.Set(ctx, "2", "2")
	tx0.Set(ctx, "3", "3")
	tx0.Set(ctx, "4", "4")
	tx0.Set(ctx, "5", "5")
	tx0.Set(ctx, "6", "6")
	tx0.Set(ctx, "7", "7")
	tx0.Set(ctx, "8", "8")
	tx0.Set(ctx, "9", "9")
	if err := tx0.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	tx := db.NewTx()
	if k, v, err := tx.FindGE(ctx, "6"); err != nil {
		t.Fatal(err)
	} else if k != "6" {
		t.Fatalf("want 6 got %s", k)
	} else {
		t.Logf("FindGE(6): %s %s %v", k, v, err)
	}

	if k, v, err := tx.FindGE(ctx, "55"); err != nil {
		t.Fatal(err)
	} else if k != "6" {
		t.Fatalf("want 6 got %s", k)
	} else {
		t.Logf("FindGE(55): %s %s %v", k, v, err)
	}

	if k, v, err := tx.FindGE(ctx, "0"); err != nil {
		t.Fatal(err)
	} else if k != "1" {
		t.Fatalf("want 1 got %s", k)
	} else {
		t.Logf("FindGE(0): %s %s %v", k, v, err)
	}

	if k, v, err := tx.FindGE(ctx, "1"); err != nil {
		t.Fatal(err)
	} else if k != "1" {
		t.Fatalf("want 1 got %s", k)
	} else {
		t.Logf("FindGE(1): %s %s %v", k, v, err)
	}

	if k, v, err := tx.FindGE(ctx, "9"); err != nil {
		t.Fatal(err)
	} else if k != "9" {
		t.Fatalf("want 9 got %s", k)
	} else {
		t.Logf("FindGE(9): %s %s %v", k, v, err)
	}

	if k, v, err := tx.FindGE(ctx, "99"); !xerrors.Is(err, os.ErrNotExist) {
		t.Fatalf("want ErrNotExist got %v", err)
	} else {
		t.Logf("FindGE(99): %s %s %v", k, v, err)
	}
}
