package cdb

import (
	"unsafe"
	"io"
	"hash"
)

const (
	// Number of hash tables
	TABLE_NUM = 256
)

// CDB
type CDB struct {
	h hash.Hash32
}

// Writer is ....
type Writer interface {
	Put(key string, keySize uint32, value string, valueSize uint32) error
	Close() error
}

// Reader is ....
type Reader interface {
	Get(key string) (string, error)
}

// hashTableRef
type hashTableRef struct {
	position, length uint32
}

// hashTable is a linearly probed open hash table
type hashTable []slot

// slot (bucket) is ..
type slot struct {
	hash, position uint32
}

func New(h hash.Hash32) *CDB {
	return &CDB{h}
}

// GetWriter
func (cdb *CDB) GetWriter(writer io.WriteSeeker) Writer {
	return newWriter(writer, cdb.h, cdb.calcTablesRefsSize())
}

func (cdb *CDB) NewReader(reader io.ReadSeeker) Reader {
	return newReader(reader, cdb.h)
}

// calcTablesRefsSize
func (cdb *CDB) calcTablesRefsSize() int64 {
	return int64(unsafe.Sizeof(hashTableRef{}) * TABLE_NUM)
}
