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

// readerImpl ... readerImpl can be share between multiple goroutines. All methods are thread safe
type readerImpl struct {
	refs   [tableNum]hashTableRef
	reader io.ReaderAt
	hasher Hasher
	endPos uint32
}

// newReader return new readerImpl object on success, otherwise return nil and error
func newReader(reader io.ReaderAt, hasher Hasher) (*readerImpl, error) {
	r := &readerImpl{
		reader: reader,
		hasher: hasher,
	}

	err := r.initialize()
	if err != nil {
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
	}

	for _, ref := range &r.refs {
		if ref.position != 0 {
			r.endPos = ref.position
			break
		}
	}

	return nil
}

// Get returns first value for given key.
func (r *readerImpl) Get(key []byte) ([]byte, error) {
	iterator, err := r.IteratorAt(key)
	if err != nil {
		return nil, err
	}

	if iterator == nil {
		return nil, nil
	}

	valueReader, n := iterator.Record().Value()
	value := make([]byte, n)
	_, err = valueReader.Read(value)

	if err != nil {
		return nil, err
	}

	return value, err
}

// Has returns true if given key exists, otherwise returns false.
func (r *readerImpl) Has(key []byte) (bool, error) {
	iterator, err := r.IteratorAt(key)

	if err != nil {
		return false, err
	}

	return iterator != nil, nil
}

// Iterator returns new Iterator object that points on first record
func (r *readerImpl) Iterator() (Iterator, error) {
	iterator := r.newIterator(tablesRefsSize, nil, nil)

	if _, err := iterator.Next(); err != nil {
		return nil, err
	}

	return iterator, nil
}

// IteratorAt returns new Iterator object that points on first record associated with given key
//
// A record is located as follows:
// * Compute the hash value of the key in the record.
// * The hash value modulo 256 (tableNum) is the number of a hash table.
// * The hash value divided by 256, modulo the length of that table, is a slot number.
// * Probe that slot, the next higher slot, and so on, until you find the record or run into an empty slot.
func (r *readerImpl) IteratorAt(key []byte) (Iterator, error) {
	h := r.calcHash(key)
	ref := &r.refs[h%tableNum]

	if ref.length == 0 {
		return nil, nil
	}

	var (
		entry        slot
		j            uint32
		valueSection sectionReaderFactory
		position     uint32
		err          error
	)

	k := (h >> 8) % ref.length

	for j = 0; j < ref.length; j++ {
		r.readPair(ref.position+k*slotSize, &entry.hash, &entry.position)

		if entry.position == 0 {
			return nil, nil
		}

		if entry.hash == h {
			valueSection, position, err = r.checkEntry(entry, key)
			if err != nil {
				return nil, err
			}

			if valueSection != nil {
				break
			}
		}

		k = (k + 1) % ref.length
	}

	if valueSection == nil {
		return nil, nil
	}

	return r.newIterator(
		position,
		func() *io.SectionReader {
			return io.NewSectionReader(bytes.NewReader(key), 0, int64(len(key)))
		},
		valueSection,
	), nil
}

// calcHash returns hash value of given key
func (r *readerImpl) calcHash(key []byte) uint32 {
	hashFunc := r.hasher()
	hashFunc.Write(key)
	return hashFunc.Sum32()
}

// checkEntry returns io.SectionReader if given slot belongs to given key, otherwise nil
func (r *readerImpl) checkEntry(entry slot, key []byte) (sectionReaderFactory, uint32, error) {
	var (
		keySize, valSize uint32
		givenKeySize     uint32 = uint32(len(key))
	)

	err := r.readPair(entry.position, &keySize, &valSize)
	if err != nil {
		return nil, 0, err
	}

	if keySize != givenKeySize {
		return nil, 0, nil
	}

	data := make([]byte, keySize)
	_, err = r.reader.ReadAt(data, int64(entry.position+8))

	if err != nil {
		return nil, 0, err
	}

	if bytes.Compare(data[:keySize], key) != 0 {
		return nil, 0, nil
	}

	position := entry.position + 8 + keySize

	return func() *io.SectionReader {
		return io.NewSectionReader(r.reader, int64(position), int64(valSize))
	}, position + valSize, nil
}

// readPair reads from r.reader uint_32 pair if possible. Returns not nil error on failure
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
func (r *readerImpl) newIterator(position uint32, keySection, valueSection sectionReaderFactory) Iterator {
	return &iterator{
		position:  position,
		cdbReader: r,
		record: &record{
			keySection:   keySection,
			valueSection: valueSection,
		},
	}
}
