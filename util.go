package kvmemdb

import (
	"path"
	"sort"
	"strings"
)

// IsCleanAbs is a database key format checker that ensures all keys are clean
// absolute paths.
func IsCleanAbs(k string) bool {
	c := path.Clean(k)
	return c == k && path.IsAbs(k)
}

// HasPrefix is a trasaction key filter that ensures a transaction can only
// read/write keys with the given prefix.
func HasPrefix(p string) func(string) bool {
	return func(k string) bool {
		return strings.HasPrefix(k, p)
	}
}

func min(a, b string) string {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a, b string) string {
	if a < b {
		return b
	} else {
		return a
	}
}

func nonempty(a, b string) string {
	if len(a) == 0 {
		return b
	} else {
		return a
	}
}

func bsearch(kvs [][2]string, key string) (int, bool) {
	i := sort.Search(len(kvs), func(x int) bool {
		return kvs[x][0] >= key
	})
	found := i < len(kvs) && kvs[i][0] == key
	return i, found
}
