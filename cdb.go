package cdb

// This library implements the data structure of the Constant Database proposed by Daniel J. Bernstein http://cr.yp.to/cdb.html

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

// OutOfMemory
var OutOfMemory = errors.New("OutOfMemory. CDB can handle any database up to 4 gigabytes")

// CDB is an associative array: it maps strings (``keys'') to strings (``data'').
type CDB struct {
	h hash.Hash32
}

// Writer provides API for creating database.
type Writer interface {
	// Put saves new associated pair <key, value> into databases. Returns not nil error on failure.
	Put(key []byte, value []byte) error
	// Commit database, make it possible for reading.
	Close() error
}

// Reader provides API for getting values by given keys. All methods are thread safe
type Reader interface {
	// Get returns first value associated with given key or returns nil if there is no associations.
	Get(key []byte) ([]byte, error)
	// Iterator returns new Iterator object that points on first record
	Iterator() (Iterator, error)
	// IteratorAt returns new Iterator object that points on first record associated with given key
	IteratorAt(key []byte) (Iterator, error)
}

// Iterator provides API for walking through database's records. Do not share object between multiple goroutines
type Iterator interface {
	// Next moves iterator to the next record. Returns true on success otherwise false
	Next() (bool, error)
	// Value returns value of current record. Returns nil if iterator is not valid
	Value() []byte
	// Key returns key of current record. Returns nil if iterator is not valid
	Key() []byte
	// IsDereferencable detects is Valid Iterator
	HasNext() bool
}

// New returns new instance of CDB struct.
func New() *CDB {
	return &CDB{&hashImpl{}}
}

// SetHash tells cdb to use given hash function for calculations.
func (cdb *CDB) SetHash(hash hash.Hash32) {
	cdb.h = hash
}

// GetWriter returns new Writer object.
func (cdb *CDB) GetWriter(writer io.WriteSeeker) (Writer, error) {
	return newWriter(writer, cdb.h)
}

// GetReader returns new Reader object.
func (cdb *CDB) GetReader(reader io.ReaderAt) (Reader, error) {
	return newReader(reader, cdb.h)
}
