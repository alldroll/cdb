// Package cdb provides interfaces for a data structure of the Constant Database
// proposed by Daniel J. Bernstein http://cr.yp.to/cdb.html
package cdb

import (
	"errors"
	"hash"
	"io"
)

const (
	// Number of hash tables
	tableNum = 256
	// Maximum value of uint32
	maxUint = 0xffffffff
	// Size of 256 tables refs
	tablesRefsSize = tableNum * 8
	// Size of hash table slot
	slotSize = 8
)

// ErrOutOfMemory tells that it was an attempt to create a cdb database up to 4 gigabytes
var ErrOutOfMemory = errors.New("OutOfMemory. CDB can handle any database up to 4 gigabytes")

// Hasher is a callback for creating a new instance of hash.Hash32.
type Hasher func() hash.Hash32

// CDB is an associative array: it maps strings (``keys'') to strings (``data'').
type CDB struct {
	Hasher
}

// Writer provides API for creating database.
type Writer interface {
	// Put saves a new associated pair <key, value> into databases. Returns an error on failure.
	Put(key []byte, value []byte) error
	// Close commits database, makes it possible for reading.
	Close() error
}

// Reader provides API for retrieving values, iterating through dataset. All methods are thread safe.
type Reader interface {
	// Get returns the first value associated with the given key
	Get(key []byte) ([]byte, error)
	// Has returns true if the given key exists, otherwise returns false.
	Has(key []byte) (bool, error)
	// Iterator returns a new Iterator object that points on the first record.
	Iterator() (Iterator, error)
	// IteratorAt returns a new Iterator object that points on the first record associated with the given key.
	IteratorAt(key []byte) (Iterator, error)
	// Size returns the size of the dataset
	Size() int
}

// Iterator provides API for iterating through database's records. Do not share object between multiple goroutines.
type Iterator interface {
	// Next moves the iterator to the next record. Returns true on success otherwise returns false.
	Next() (bool, error)
	// Record returns the current record. This method is lazy. It means, a data is read on require.
	Record() Record
	// HasNext tells if the iterator can be moved to the next record.
	HasNext() bool
	// Key returns key's []byte slice. It is usually easier to use and
	// faster then iterator.Record().Key().
	// Because it doesn't requiers allocation for record copy.
	Key() ([]byte, error)
	// ValueBytes returns values's []byte slice. It is usually easier to use and
	// faster then iterator.Record().Key().
	// Because it doesn't requiers allocation for record copy.
	Value() ([]byte, error)
}

// Record provides API for reading record key, value.
type Record interface {
	// Key returns io.Reader with given record's key and key size.
	Key() (io.Reader, uint32)
	// Value returns io.Reader with given record's value and value size.
	Value() (io.Reader, uint32)
}

// New returns a new instance of CDB struct.
func New() *CDB {
	return &CDB{NewHash}
}

// SetHash tells the cdb to use the given hash function for calculations.
// Given hash will be used only for new instances of Reader, Writer.
//
// h := cdb.New()
// h.GetWriter(f) ... do some work ( here we use default Hasher )
// h.SetHash(fnv.Hash32)
// h.GetReader(f)  - only new instances will be use fnv.Hash32
func (cdb *CDB) SetHash(hasher Hasher) {
	cdb.Hasher = hasher
}

// GetWriter returns a new Writer object.
func (cdb *CDB) GetWriter(writer io.WriteSeeker) (Writer, error) {
	return newWriter(writer, cdb.Hasher)
}

// GetReader returns a new Reader object.
func (cdb *CDB) GetReader(reader io.ReaderAt) (Reader, error) {
	return newReader(reader, cdb.Hasher)
}
