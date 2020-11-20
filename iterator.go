package cdb

import (
	"errors"
	"io"
)

var ErrEmptyCDB = errors.New("cdb is empty")

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

// readSection reads current record. Returns []byte and error
func readSection(readerAt io.ReaderAt, position int64, size uint32) ([]byte, error) {
	val := make([]byte, size)
	readSize, err := readerAt.ReadAt(val, position)
	if err != nil {
		if err == io.EOF && readSize == int(size) {
			return val, nil
		}
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
	keyFactory := i.record.keySectionFactory
	return readSection(keyFactory.reader, int64(keyFactory.position), keyFactory.size)
}

// ValueBytes returns values's []byte slice. It is usually easier to use and
// faster then Value(). Because it doesn't requiers allocation for SectionReader
func (i *iterator) Value() ([]byte, error) {
	valueFactory := i.record.valueSectionFactory
	return readSection(valueFactory.reader, int64(valueFactory.position), valueFactory.size)
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
	if i.cdbReader.IsEmpty() {
		return false
	}
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
