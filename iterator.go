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

// read reads current record. Returns []byte and error
func (s *sectionReaderFactory) read() ([]byte, error) {
	recSize := s.size
	val := make([]byte, recSize)
	_, err := s.reader.ReadAt(val, int64(s.position))
	if err != nil {
		return nil, err
	}
	return val, nil
}

// Next moves the iterator to the next record. Returns true on success otherwise returns false.
func (i *iterator) Next() (bool, error) {

	if !i.HasNext() {
		return false, nil
	}

	var keySize, valSize uint32

	if err := i.cdbReader.readPair(i.position, &keySize, &valSize); err != nil {
		return false, err
	}

	i.record.keySectionFactory.position = i.position + 8
	i.record.keySectionFactory.size = keySize

	i.record.valueSectionFactory.position = i.position + 8 + keySize
	i.record.valueSectionFactory.size = valSize

	i.position += keySize + valSize + 8

	return true, nil
}

// KeyBytes returns key's []byte slice. It is usually easier to use and
// faster then Key(). Because it doesn't requiers allocation for SectionReader
func (i *iterator) Key() ([]byte, error) {
	return i.record.keySectionFactory.read()
}

// ValueBytes returns values's []byte slice. It is usually easier to use and
// faster then Value(). Because it doesn't requiers allocation for SectionReader
func (i *iterator) Value() ([]byte, error) {
	return i.record.valueSectionFactory.read()
}

// Record returns copy of current record
func (i *iterator) Record() Record {
	return &record{
		keySectionFactory: &sectionReaderFactory{
			reader:   i.record.keySectionFactory.reader,
			position: i.record.keySectionFactory.position,
			size:     i.record.keySectionFactory.size,
		},
		valueSectionFactory: &sectionReaderFactory{
			reader:   i.record.valueSectionFactory.reader,
			position: i.record.valueSectionFactory.position,
			size:     i.record.valueSectionFactory.size,
		},
	}
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
