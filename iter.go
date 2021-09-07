package kvmemdb

import (
	"context"
	"os"
)

type Iter struct {
	tx *Tx

	i, j int

	ascending bool

	kvs [][2]string
}

// GetNext returns next element of the iterator. Returns os.ErrNotExist when
// reaches to the end.
func (it *Iter) GetNext(ctx context.Context) (string, string, error) {
	if len(it.kvs) == 0 {
		return "", "", os.ErrNotExist
	}

	if it.ascending {
		if it.i > it.j {
			return "", "", os.ErrNotExist
		}
		k, v := it.kvs[it.i][0], it.kvs[it.i][1]
		it.tx.touch(k)
		it.i++
		return k, v, nil
	}

	if it.i < it.j {
		return "", "", os.ErrNotExist
	}
	k, v := it.kvs[it.i][0], it.kvs[it.i][1]
	it.tx.touch(k)
	it.i--
	return k, v, nil
}
