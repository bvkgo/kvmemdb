package kvmemdb

import "sort"

// kvMap holds key-value pairs, including the delete items, sorted in
// increasing order of the key names.
type kvMap []kvPair

func (m kvMap) Clone() kvMap {
	n := kvMap(make([]kvPair, 0, len(m)))
	for _, kv := range m {
		n = append(n, kv)
	}
	return n
}

// Index returns the index for the matching key-value pair. Returns false with
// the expected index of a pair if it was present which could range between [0,
// len(m)] inclusive.
func (m kvMap) Index(k string) (int, bool) {
	i := sort.Search(len(m), func(i int) bool {
		return m[i].key >= k
	})
	if i < len(m) {
		if m[i].key == k {
			return i, true
		}
		return i, false
	}
	return len(m), false
}

func (m kvMap) Get(k string) (kvPair, bool) {
	if i, ok := m.Index(k); ok {
		return m[i], true
	}
	return kvPair{}, false
}

func (m kvMap) Set(p kvPair) kvMap {
	if i, ok := m.Index(p.key); ok {
		m[i] = p
		return m
	}
	m = append(m, p)
	sort.Slice(m, func(i, j int) bool {
		return m[i].key < m[j].key
	})
	return m
}
