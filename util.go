package kvmemdb

import (
	"path"
	"strings"
)

// IsNonEmpty is a KeyChecker that disallows empty keys.
func IsNonEmpty(k string) bool {
	return k != ""
}

// IsCleanRel is a KeyChecker that ensures key represents a clean path and is
// not an absolute starting at "/".
func IsCleanRel(k string) bool {
	c := path.Clean(k)
	return c == k && !path.IsAbs(k)
}

// IsCleanAbs is a KeyChecker that ensures key represents a clean absolute path.
func IsCleanAbs(k string) bool {
	c := path.Clean(k)
	return c == k && path.IsAbs(k)
}

// HasPrefix is a trasaction filter that ensures a transaction can only
// read/write to keys with the given prefix.
func HasPrefix(p string) func(string) bool {
	return func(k string) bool {
		return strings.HasPrefix(k, p)
	}
}
