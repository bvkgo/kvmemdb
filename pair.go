package kvmemdb

import "time"

type kvPair struct {
	key   string
	value string

	// ctime and mtime keep track of the key-value creation, modification and
	// deletion timestamps.
	//
	// New key-value items will have a non-zero ctime with mtime == ctime.
	//
	// Modifications will have ctime before mtime.
	//
	// Tombstones will have mtime == ctime-nanosecond, while Deletions will have
	// zero value mtime.
	ctime, mtime time.Time
}

func newPair(k, v string) kvPair {
	now := time.Now()
	return kvPair{
		key:   k,
		value: v,
		ctime: now,
		mtime: now,
	}
}

func newTomb(k string) kvPair {
	now := time.Now()
	return kvPair{
		key:   k,
		value: "",
		ctime: now,
		mtime: now.Add(-time.Nanosecond),
	}
}

func (p kvPair) isTomb() bool {
	return p.ctime.Sub(p.mtime) == time.Nanosecond
}

func (p kvPair) isDeleted() bool {
	return p.mtime.IsZero() || p.mtime.Before(p.ctime)
}

func (p kvPair) isNew() bool {
	return !p.ctime.IsZero() && p.mtime.Equal(p.ctime)
}

func (p kvPair) isEqual(other kvPair) bool {
	return p.ctime.Equal(other.ctime) && p.mtime.Equal(other.mtime) && p.key == other.key && p.value == other.value
}

func (p kvPair) withCommitTime(t time.Time) kvPair {
	if p.isTomb() {
		return kvPair{
			key:   p.key,
			value: "",
			ctime: t,
			mtime: t.Add(-time.Nanosecond),
		}
	}
	if p.isDeleted() {
		return kvPair{
			key:   p.key,
			value: "",
			ctime: t,
		}
	}
	if p.isNew() {
		return kvPair{
			key:   p.key,
			value: p.value,
			ctime: t,
			mtime: t,
		}
	}
	return kvPair{
		key:   p.key,
		value: p.value,
		ctime: p.ctime,
		mtime: t,
	}
}
