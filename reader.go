package cdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"sync"
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
	hash   hash.Hash32
	mutex  sync.Mutex
	endPos uint32
}

// newReader return new readerImpl object on success, otherwise return nil and error
func newReader(reader io.ReaderAt, h hash.Hash32) (*readerImpl, error) {
	r := &readerImpl{
		reader: reader,
		hash:   h,
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

	for i := range r.refs {
		j := i * 8
		r.refs[i].position, r.refs[i].length = binary.LittleEndian.Uint32(buf[j:j+4]), binary.LittleEndian.Uint32(buf[j+4:j+8])
	}

	for _, ref := range r.refs {
		if ref.position != 0 {
			r.endPos = ref.position
			break
		}
	}

	return nil
}

// A record is located as follows:
// * Compute the hash value of the key in the record.
// * The hash value modulo 256 (tableNum) is the number of a hash table.
// * The hash value divided by 256, modulo the length of that table, is a slot number.
// * Probe that slot, the next higher slot, and so on, until you find the record or run into an empty slot.
//
// Get returns first value for given key.
func (r *readerImpl) Get(key []byte) ([]byte, error) {
	iterator, err := r.IteratorAt(key)
	if err != nil {
		return nil, err
	}

	if iterator == nil {
		return nil, nil
	}

	return iterator.Value(), nil
}

// Iterator returns new Iterator object that points on first record
func (r *readerImpl) Iterator() (Iterator, error) {
	iterator := r.newIterator(tablesRefsSize, nil, nil)

	_, err := iterator.Next()
	if err != nil {
		return nil, err
	}

	return iterator, nil
}

// IteratorAt returns new Iterator object that points on first record associated with given key
func (r *readerImpl) IteratorAt(key []byte) (Iterator, error) {
	h := r.calcHash(key)
	ref := r.refs[h%tableNum]

	if ref.length == 0 {
		return nil, nil
	}

	var (
		entry slot
		j     uint32
	)

	k := (h >> 8) % ref.length

	for j = 0; j < ref.length; j++ {
		r.readPair(ref.position+k*slotSize, &entry.hash, &entry.position)

		if entry.position == 0 {
			return nil, nil
		}

		if entry.hash == h {
			value, err := r.readValue(entry, key)
			if err != nil {
				return nil, err
			}

			if value != nil {
				return r.newIterator(entry.position+uint32(len(key))+uint32(len(value)+8), key, value), nil
			}
		}

		k = (k + 1) % ref.length
	}

	return nil, nil
}

// calcHash returns hash value of given key
func (r *readerImpl) calcHash(key []byte) uint32 {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.hash.Write(key)
	return r.hash.Sum32()
}

// readValue returns value of given entry if entry key is same with given key
func (r *readerImpl) readValue(entry slot, key []byte) ([]byte, error) {
	var (
		keySize, valSize uint32
		givenKeySize     uint32 = uint32(len(key))
	)

	err := r.readPair(entry.position, &keySize, &valSize)
	if err != nil {
		return nil, err
	}

	if keySize != givenKeySize {
		return nil, nil
	}

	data := make([]byte, keySize+valSize)
	_, err = r.reader.ReadAt(data, int64(entry.position+8))

	if err != nil {
		return nil, err
	}

	if bytes.Compare(data[:keySize], key) != 0 {
		return nil, nil
	}

	return data[keySize:], nil
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
func (r *readerImpl) newIterator(position uint32, key, value []byte) Iterator {
	return &iteratorImpl{
		position,
		r,
		key,
		value,
		r.endPos > position,
	}
}
