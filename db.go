package kvmemdb

import (
	"sync"
	"time"

	"golang.org/x/xerrors"
)

type DB struct {
	// KeyChecker, when not nil, enforces user-defined rules on the key
	// format. Key format is enforced only on insertions into the database, i.e.,
	// only by a transaction's Set and Delete operations.
	KeyChecker func(string) bool

	mu   sync.RWMutex
	data kvMap
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
// modifications to any one of them will fail the transaction commit. So, use
// them sparingly. Find* functions can help avoid reading unnecessary keys.
func (db *DB) NewTx() *Tx {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return &Tx{db: db, snap: db.data.Clone()}
}

// NewFilteredTx creates a transaction with restricted access. Returned
// transaction can only read/write keys that are allowed by the input filter.
//
// Filters complement the KeyChecker of the DB with additional control. For
// example, a KeyChecker can be used to enforce a file path structure on all
// keys in the key-value store and transaction filters can limit transactions
// to specific subdirectories.
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
	if db.KeyChecker == nil {
		return true
	}
	return db.KeyChecker(k)
}

func (db *DB) tryCommit(tx *Tx) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	at := time.Now()

	// Check that all items accessed by the transaction are unmodified in the
	// database.
	for i := range tx.accessed {
		key := tx.accessed[i].key
		snap, snapOK := tx.snap.Get(key)
		curr, currOK := db.data.Get(key)

		// A new key-value pair is created by this transaction. This key-value
		// pair didn't exist when the transaction has begun and also doesn't exist
		// when the transaction is to be committed.
		if !snapOK && !currOK {
			continue
		}

		// Some other transaction has created a key-value item with same key name
		// as this transaction.
		if !snapOK && currOK {
			return xerrors.Errorf("key %q is also created/deleted by another tx", key)
		}

		// Some other transaction has deleted a key-value item accessed by this
		// transaction.
		if snapOK && !currOK {
			return xerrors.Errorf("key %q is deleted/deleted by another tx", key)
		}

		// Existing key-value pair is not updated or recreated by some other
		// transaction.
		if !snap.isEqual(curr) {
			return xerrors.Errorf("key %q is updated or recreated by another tx", key)
		}
	}

	// Update the database with with the items from the transaction. TODO: We can
	// avoid sorting multiple times.
	for _, access := range tx.accessed {
		db.data = db.data.Set(access.withCommitTime(at))
	}
	return nil
}
