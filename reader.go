package cdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

// hashTableRef is a pointer that state a position and a length of the hash table
// position is the starting byte position of the hash table.
// The length is the number of slots in the hash table.
type hashTableRef struct {
	position, length uint32
}

// readerImpl implements Reader interface
type readerImpl struct {
	refs   [tableNum]hashTableRef
	reader io.ReaderAt
	hasher Hasher
	endPos uint32
	size   int
}

// newReader returns a new readerImpl object on success, otherwise returns nil and an error
func newReader(reader io.ReaderAt, hasher Hasher) (*readerImpl, error) {
	r := &readerImpl{
		reader: reader,
		hasher: hasher,
	}

	if err := r.initialize(); err != nil {
		return nil, err
	}

	return r, nil
}

// initialize reads hashTableRefs from r.reader
func (r *readerImpl) initialize() error {
	buf := make([]byte, tablesRefsSize)
	_, err := r.reader.ReadAt(buf, 0)

	if err != nil {
		return errors.New("Invalid db header, impossible to read hashTableRefs structures")
	}

	for i := range &r.refs {
		j := i * 8
		r.refs[i].position, r.refs[i].length = binary.LittleEndian.Uint32(buf[j:j+4]), binary.LittleEndian.Uint32(buf[j+4:j+8])
		r.size += int(r.refs[i].length >> 1)
	}

	for _, ref := range &r.refs {
		if ref.position != 0 {
			r.endPos = ref.position
			break
		}
	}

	return nil
}

// Get returns the first value associated with the given key
func (r *readerImpl) Get(key []byte) ([]byte, error) {
	valueSection, err := r.findEntry(key)

	if valueSection == nil || err != nil {
		return nil, err
	}

	value := make([]byte, valueSection.size)

	if _, err = valueSection.reader.ReadAt(value, int64(valueSection.position)); err != nil {
		return nil, err
	}

	return value, nil
}

// Has returns true if the given key exists, otherwise returns false.
func (r *readerImpl) Has(key []byte) (bool, error) {
	valueSection, err := r.findEntry(key)

	return valueSection != nil, err
}

// Iterator returns new Iterator object that points on first record
func (r *readerImpl) Iterator() (Iterator, error) {
	iterator := r.newIterator(tablesRefsSize, nil, nil)

	if _, err := iterator.Next(); err != nil {
		return nil, err
	}

	return iterator, nil
}

// IteratorAt returns a new Iterator object that points on the first record associated with the given key.
func (r *readerImpl) IteratorAt(key []byte) (Iterator, error) {
	valueSection, err := r.findEntry(key)

	if err != nil || valueSection == nil {
		return nil, err
	}

	return r.newIterator(
		valueSection.position+valueSection.size,
		&sectionReaderFactory{
			reader: bytes.NewReader(key),
			size:   uint32(len(key)),
		},
		valueSection,
	), nil
}

// Size returns the size of the dataset
func (r *readerImpl) Size() int {
	return r.size
}

// findEntry finds an entry for the given key
//
// A record is located as follows:
// * Compute the hash value of the key in the record.
// * The hash value modulo 256 (tableNum) is the number of a hash table.
// * The hash value divided by 256, modulo the length of that table, is a slot number.
// * Probe that slot, the next higher slot, and so on, until you find the record or run into an empty slot.
func (r *readerImpl) findEntry(key []byte) (*sectionReaderFactory, error) {
	h := r.calcHash(key)
	ref := &r.refs[h%tableNum]

	if ref.length == 0 {
		return nil, nil
	}

	var (
		entry        slot
		j            uint32
		valueSection *sectionReaderFactory
		err          error
	)

	k := (h >> 8) % ref.length

	for j = 0; j < ref.length; j++ {
		r.readPair(ref.position+k*slotSize, &entry.hash, &entry.position)

		if entry.position == 0 {
			return nil, nil
		}

		if entry.hash == h {
			valueSection, err = r.checkEntry(entry, key)

			if err != nil {
				return nil, err
			}

			if valueSection != nil {
				break
			}
		}

		k = (k + 1) % ref.length
	}

	return valueSection, nil
}

// calcHash returns hash value of given key
func (r *readerImpl) calcHash(key []byte) uint32 {
	hashFunc := r.hasher()
	hashFunc.Write(key)

	return hashFunc.Sum32()
}

// checkEntry returns io.SectionReader if given slot belongs to given key, otherwise nil
func (r *readerImpl) checkEntry(entry slot, key []byte) (*sectionReaderFactory, error) {
	var (
		keySize, valSize uint32
		givenKeySize     = uint32(len(key))
	)

	if err := r.readPair(entry.position, &keySize, &valSize); err != nil {
		return nil, err
	}

	if keySize != givenKeySize {
		return nil, nil
	}

	data := make([]byte, keySize)

	if _, err := r.reader.ReadAt(data, int64(entry.position+8)); err != nil {
		return nil, err
	}

	if bytes.Compare(data, key) != 0 {
		return nil, nil
	}

	return &sectionReaderFactory{
		reader:   r.reader,
		position: entry.position + 8 + keySize,
		size:     valSize,
	}, nil
}

// readPair reads from r.reader uint_32 pair if possible. Returns an error on failure
func (r *readerImpl) readPair(pos uint32, a, b *uint32) error {
	pair := make([]byte, 8, 8)

	_, err := r.reader.ReadAt(pair, int64(pos))
	if err != nil {
		return err
	}

	*a, *b = binary.LittleEndian.Uint32(pair), binary.LittleEndian.Uint32(pair[4:])

	return nil
}

// newIterator returns new instance of Iterator object
func (r *readerImpl) newIterator(position uint32, keySectionFactory, valueSectionFactory *sectionReaderFactory) Iterator {
	return &iterator{
		position:  position,
		cdbReader: r,
		record: &record{
			keySectionFactory:   keySectionFactory,
			valueSectionFactory: valueSectionFactory,
		},
	}
}
