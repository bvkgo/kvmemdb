package kvmemdb

import (
	"context"
	"os"
	"testing"

	"golang.org/x/xerrors"
)

func TestFindLE(t *testing.T) {
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

	tx1 := db.NewTx()
	tx1.Set(ctx, "00", "00")
	tx1.Set(ctx, "11", "11")
	tx1.Set(ctx, "22", "22")
	tx1.Set(ctx, "33", "33")
	tx1.Set(ctx, "44", "44")
	tx1.Set(ctx, "55", "55")
	tx1.Set(ctx, "66", "66")
	tx1.Set(ctx, "77", "77")
	tx1.Set(ctx, "88", "88")
	tx1.Set(ctx, "99", "99")

	if k, v, err := tx1.FindLE(ctx, "0"); !xerrors.Is(err, os.ErrNotExist) {
		t.Fatalf("want ErrNotExists got %s,%s,%v", k, v, err)
	}

	if k, v, err := tx1.FindLE(ctx, "00"); err != nil {
		t.Fatal(err)
	} else if k != "00" {
		t.Fatalf("want 00 got %s", k)
	} else {
		t.Logf("FindLE(00): %q %q", k, v)
	}

	if k, v, err := tx1.FindLE(ctx, "000"); err != nil {
		t.Fatal(err)
	} else if k != "00" {
		t.Fatalf("want 00 got %s", k)
	} else {
		t.Logf("FindLE(000): %q %q", k, v)
	}

	if k, v, err := tx1.FindLE(ctx, "45"); err != nil {
		t.Fatal(err)
	} else if k != "44" {
		t.Fatalf("want 44 got %s", k)
	} else {
		t.Logf("FindLE(45): %q %q", k, v)
	}

	if k, v, err := tx1.FindLE(ctx, "999"); err != nil {
		t.Fatal(err)
	} else if k != "99" {
		t.Fatalf("want 99 got %s", k)
	} else {
		t.Logf("FindLE(999): %q %q", k, v)
	}

	if k, v, err := tx1.FindLE(ctx, "99"); err != nil {
		t.Fatal(err)
	} else if k != "99" {
		t.Fatalf("want 99 got %s", k)
	} else {
		t.Logf("Find(99): %q %q", k, v)
	}

	if k, v, err := tx1.FindLE(ctx, "98"); err != nil {
		t.Fatal(err)
	} else if k != "9" {
		t.Fatalf("want 9 got %s", k)
	} else {
		t.Logf("Find(98): %q %q", k, v)
	}
}

func TestFindLEWithDeletions(t *testing.T) {
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

	tx1 := db.NewTx()
	tx1.Set(ctx, "00", "00")
	tx1.Set(ctx, "11", "11")
	tx1.Set(ctx, "22", "22")
	tx1.Set(ctx, "33", "33")
	tx1.Set(ctx, "44", "44")
	tx1.Set(ctx, "55", "55")
	tx1.Set(ctx, "66", "66")
	tx1.Set(ctx, "77", "77")
	tx1.Set(ctx, "88", "88")
	tx1.Set(ctx, "99", "99")

	if k, v, err := tx1.FindLE(ctx, "0"); !xerrors.Is(err, os.ErrNotExist) {
		t.Fatalf("want ErrNotExists got %s,%s,%v", k, v, err)
	}

	if k, v, err := tx1.FindLE(ctx, "00"); err != nil {
		t.Fatal(err)
	} else if k != "00" {
		t.Fatalf("want 00 got %s", k)
	} else {
		t.Logf("FindLE(00): %q %q", k, v)
	}

	if k, v, err := tx1.FindLE(ctx, "000"); err != nil {
		t.Fatal(err)
	} else if k != "00" {
		t.Fatalf("want 00 got %s", k)
	} else {
		t.Logf("FindLE(000): %q %q", k, v)
	}

	if k, v, err := tx1.FindLE(ctx, "45"); err != nil {
		t.Fatal(err)
	} else if k != "44" {
		t.Fatalf("want 44 got %s", k)
	} else {
		t.Logf("FindLE(45): %q %q", k, v)
	}

	if k, v, err := tx1.FindLE(ctx, "999"); err != nil {
		t.Fatal(err)
	} else if k != "99" {
		t.Fatalf("want 99 got %s", k)
	} else {
		t.Logf("FindLE(999): %q %q", k, v)
	}

	if k, v, err := tx1.FindLE(ctx, "99"); err != nil {
		t.Fatal(err)
	} else if k != "99" {
		t.Fatalf("want 99 got %s", k)
	} else {
		t.Logf("Find(99): %q %q", k, v)
	}

	if k, v, err := tx1.FindLE(ctx, "98"); err != nil {
		t.Fatal(err)
	} else if k != "9" {
		t.Fatalf("want 9 got %s", k)
	} else {
		t.Logf("Find(98): %q %q", k, v)
	}
}
