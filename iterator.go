package cdb

import (
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
	valueSectionFactory *sectionReaderFactory
	keySectionFactory   *sectionReaderFactory
}

type sectionReaderFactory struct {
	reader         io.ReaderAt
	position, size uint32
}

func (s *sectionReaderFactory) create() (io.Reader, uint32) {
	return io.NewSectionReader(s.reader, int64(s.position), int64(s.size)), s.size
}

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

	i.record = &record{
		keySectionFactory: &sectionReaderFactory{
			reader:   i.cdbReader.reader,
			position: i.position + 8,
			size:     keySize,
		},
		valueSectionFactory: &sectionReaderFactory{
			reader:   i.cdbReader.reader,
			position: i.position + 8 + keySize,
			size:     valSize,
		},
	}

	i.position += keySize + valSize + 8

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
func (r *record) Key() (io.Reader, uint32) {
	return r.keySectionFactory.create()
}

// Value returns io.SectionReader which points on record's value
func (r *record) Value() (io.Reader, uint32) {
	return r.valueSectionFactory.create()
}
