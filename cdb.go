package cdb

// This library implements the data structure of the Constant Database proposed by Daniel J. Bernstein http://cr.yp.to/cdb.html

import (
	"io"
	"hash"
)

const (
	// Number of hash tables
	TABLE_NUM = 256
)

// CDB is an associative array: it maps strings (``keys'') to strings (``data'').
type CDB struct {
	h hash.Hash32
}

// Writer is ....
type Writer interface {
	Put(key []byte, value []byte) error
	Close() error
}

// Reader is ....
type Reader interface {
	Get(key []byte) ([]byte, error)
}

func New(h hash.Hash32) *CDB {
	return &CDB{h}
}

// GetWriter
func (cdb *CDB) GetWriter(writer io.WriteSeeker) Writer {
	return newWriter(writer, cdb.h)
}

func (cdb *CDB) GetReader(reader io.ReadSeeker) (Reader, error) {
	return newReader(reader, cdb.h)
}
