package cdb

import (
	"io"
)

// iterator implements Iterator interface
type iterator struct {
	position  uint32
	cdbReader *readerImpl
	record    *record
}

// record implements Record interface
type record struct {
	valueSectionFactory *sectionReaderFactory
	keySectionFactory   *sectionReaderFactory
}

// sectionReaderFactory is a factory for creating a NewSectionReader
type sectionReaderFactory struct {
	reader         io.ReaderAt
	position, size uint32
}

// create returns a new instance of SectionReader
func (s *sectionReaderFactory) create() (io.Reader, uint32) {
	return io.NewSectionReader(s.reader, int64(s.position), int64(s.size)), s.size
}

// Next moves the iterator to the next record. Returns true on success otherwise returns false.
func (i *iterator) Next() (bool, error) {
	i.record = nil

	if !i.HasNext() {
		return false, nil
	}

	var keySize, valSize uint32

	if err := i.cdbReader.readPair(i.position, &keySize, &valSize); err != nil {
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

// Record returns the current record
func (i *iterator) Record() Record {
	return i.record
}

// HasNext tells if the iterator can be moved to the next record.
func (i *iterator) HasNext() bool {
	return i.position < i.cdbReader.endPos
}

// Key returns io.Reader with given record's key and key size.
func (r *record) Key() (io.Reader, uint32) {
	return r.keySectionFactory.create()
}

// Value returns io.Reader with given record's value and value size.
func (r *record) Value() (io.Reader, uint32) {
	return r.valueSectionFactory.create()
}
