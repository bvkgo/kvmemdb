package kvmemdb

import (
	"path"
	"strings"
)

// IsNonEmpty is a database key format check that disallows empty keys.
func IsNonEmpty(k string) bool {
	return k != ""
}

// IsCleanRel is a database key format checker that ensures all keys are clean,
// relative paths.
func IsCleanRel(k string) bool {
	c := path.Clean(k)
	return c == k && !path.IsAbs(k)
}

// IsCleanAbs is a database key format checker that ensures all keys are clean
// absolute paths.
func IsCleanAbs(k string) bool {
	c := path.Clean(k)
	return c == k && path.IsAbs(k)
}

// HasPrefix is a trasaction filter that ensures a transaction can only
// read/write keys with the given prefix.
func HasPrefix(p string) func(string) bool {
	return func(k string) bool {
		return strings.HasPrefix(k, p)
	}
}
