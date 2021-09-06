package kvmemdb

import (
	"fmt"
	"sync"
	"time"
)

type DB struct {
	// checker, when not nil, enforces user-defined rules on the key
	// format. Key format is enforced only on insertions into the database, i.e.,
	// only by a transaction's Set and Delete operations.
	checker func(string) bool

	mu   sync.RWMutex
	data kvMap
}

// New creates an in-memory database with a key format checker.
func New(keyChecker func(string) bool) *DB {
	return &DB{checker: keyChecker}
}

// NewTx creates a read-write transaction on the database. Multiple
// transactions can be created in parallel.
//
// Transaction take a snapshot of the database at the time of their creation,
// so updates within one transaction are not visible to other transactions.
//
// When a transaction is committed, all keys accessed by the transaction are
// verified against the current state of the database. If keys accessed by the
// transaction are unmodified in the database, between the trasaction creation
// and commit time points, then commit will succeed.
//
// Note that, transaction operations that failed with os.ErrNotExist error are
// NOT interpreted as key accesses when committing the transaction.
//
// Note that Scan/Ascend/Descend functions could access huge number of keys, so
// modifications to any one of them will fail the transaction commit.
func (db *DB) NewTx() *Tx {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return &Tx{db: db, snap: db.data.Clone()}
}

// NewFilteredTx creates a transaction with restricted access. Returned
// transaction can only read/write keys that are allowed by the filter.
//
// Filters complement the database key format checker with additional
// control. For example, a key format checker can be used to enforce a file
// path structure on all keys in the key-value store and transaction filters
// can limit transactions to specific subdirectories.
func (db *DB) NewFilteredTx(filter func(string) bool) *Tx {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return &Tx{
		db:     db,
		snap:   db.data.Clone(),
		filter: filter,
	}
}

func (db *DB) checkKey(k string) bool {
	if len(k) == 0 {
		return false
	}
	if db.checker == nil {
		return true
	}
	return db.checker(k)
}

func (db *DB) tryCommit(tx *Tx) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	at := time.Now()

	// Check that all items accessed by the transaction are unmodified in the
	// database.
	var modified []int
	for i, kv := range tx.accessed {
		snap, snapOK := tx.snap.Get(kv.key)
		curr, currOK := db.data.Get(kv.key)

		// A new key-value pair is created by this transaction. This key-value
		// pair didn't exist when the transaction has begun and also doesn't exist
		// when the transaction is to be committed.
		if !snapOK && !currOK {
			modified = append(modified, i)
			continue
		}

		// Some other transaction has created a key-value item with same key name
		// as this transaction.
		if !snapOK && currOK {
			return fmt.Errorf("key %q is also created/deleted by another tx", kv.key)
		}

		// Some other transaction has deleted a key-value item accessed by this
		// transaction.
		if snapOK && !currOK {
			return fmt.Errorf("key %q is deleted/deleted by another tx", kv.key)
		}

		// Existing key-value pair is not updated or recreated by some other
		// transaction.
		if !snap.isEqual(curr) {
			return fmt.Errorf("key %q is updated or recreated by another tx", kv.key)
		}

		// Check if value is modified or not.
		if !curr.isEqual(kv) {
			modified = append(modified, i)
		}
	}

	// Update the database with with the items from the transaction. TODO: We can
	// avoid sorting multiple times.
	for i := range modified {
		db.data = db.data.Set(tx.accessed[i].withCommitTime(at))
	}
	return nil
}
