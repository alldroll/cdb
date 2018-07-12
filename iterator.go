package cdb

import (
	"bytes"
	"io"
)

// iterator represents implementation of Iterator
type iterator struct {
	position  uint32
	cdbReader *readerImpl
	record    *record
}

// record represents implementation of Record
type record struct {
	valueSection sectionReaderFactory
	keySection   sectionReaderFactory
}

type sectionReaderFactory func() *io.SectionReader

// Next moves iterator to the next record. Returns true on success otherwise false
func (i *iterator) Next() (bool, error) {
	i.record = nil

	if !i.HasNext() {
		return false, nil
	}

	var keySize, valSize uint32

	err := i.cdbReader.readPair(i.position, &keySize, &valSize)
	if err != nil {
		return false, err
	}

	buf := make([]byte, keySize+valSize)
	i.cdbReader.reader.ReadAt(buf, int64(i.position+8))
	i.position += keySize + valSize + 8

	i.record = &record{
		keySection: func() *io.SectionReader {
			return io.NewSectionReader(bytes.NewReader(buf[:keySize]), 0, int64(keySize))
		},
		valueSection: func() *io.SectionReader {
			return io.NewSectionReader(bytes.NewReader(buf[keySize:]), 0, int64(valSize))
		},
	}

	return true, nil
}

// Record returns record on which points given iterator
func (i *iterator) Record() Record {
	return i.record
}

// HasNext tells can iterator be moved to the next record.
func (i *iterator) HasNext() bool {
	return i.position < i.cdbReader.endPos
}

// Key returns io.SectionReader which points on record's key
func (r *record) Key() (io.Reader, int) {
	reader := r.keySection()
	return reader, int(reader.Size())
}

// Value returns io.SectionReader which points on record's value
func (r *record) Value() (io.Reader, int) {
	reader := r.valueSection()
	return reader, int(reader.Size())
}
