package kvmemdb

import (
	"context"
	"strings"
	"testing"
)

func TestRelativeKeys(t *testing.T) {
	ctx := context.Background()

	keys := []string{
		"/etc",
		"/tmp/go-build354887057/b050/vet.cfg",
		"/tmp/go-build354887057/b050/_pkg_.a",
		"/tmp/go-build354887057/b050/importcfg",
		"/tmp/.Test-unix",
		"/tmp/systemd-private-72c19116405b4207b4ef8ae2004b3983-apache2.service-Ahp4qC",
		"/tmp/.XIM-unix",
		"/tmp/.X11-unix",
		"/tmp/.X11-unix/X0",
		"/tmp/.ICE-unix",
		"/usr",
	}

	var db DB
	{
		tx := db.NewTx()
		for _, p := range keys {
			if err := tx.Set(ctx, p, p); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}

	tx := db.NewTx()
	defer tx.Rollback(ctx)

	count := 0
	for last := "/tmp/"; true; {
		file, _, err := tx.FindGT(ctx, last)
		if err != nil || !strings.HasPrefix(file, "/tmp/") {
			break
		}
		count++
		last = file
	}

	if l := len(keys); count != l-2 {
		t.Fatalf("wanted %d got %d", l-2, count)
	}
}
