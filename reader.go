package cdb

import (
	"io"
	"hash"
	"encoding/binary"
	"bytes"
	"sync"
)

// hashTableRef is a pointer that state a position and a length of the hash table
// position is the starting byte position of the hash table.
// The length is the number of slots in the hash table.
type hashTableRef struct {
	position, length uint32
}

type readerImpl struct {
	refs [TABLE_NUM]hashTableRef
	reader io.ReaderAt
	hash hash.Hash32
	mutex sync.Mutex
}

func newReader(reader io.ReaderAt, h hash.Hash32) (*readerImpl, error) {
	r := &readerImpl{
		reader: reader,
		hash: h,
	}

	err := r.open()
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (r *readerImpl) open() error {
	buf := make([]byte, calcTablesRefsSize())
	r.reader.ReadAt(buf, 0)

	for i, _ := range r.refs {
		j := i * 8
		r.refs[i].position, r.refs[i].length = binary.LittleEndian.Uint32(buf[j: j+4]), binary.LittleEndian.Uint32(buf[j+4: j+8])
	}

	return nil
}

//
// A record is located as follows. Compute the hash value of the key in
// the record. The hash value modulo 256 (TABLE_NUM) is the number of a hash table.
// The hash value divided by 256, modulo the length of that table, is a
// slot number. Probe that slot, the next higher slot, and so on, until
// you find the record or run into an empty slot.
func (r *readerImpl) Get(key []byte) ([]byte, error) {
	h := r.calcHash(key)
	ref := r.refs[h % TABLE_NUM]

	if ref.length == 0 {
		return nil, nil
	}

	var (
		entry slot
		j uint32
	)

	k := (h >> 8) % ref.length
	slotSize := calcSlotSize()

	for j = 0; j < ref.length; j++ {
		r.readPair(ref.position + k * slotSize, &entry.hash, &entry.position)

		if entry.position == 0 {
			return nil, nil
		}

		if entry.hash == h {
			value, err := r.readValue(entry, key)
			if err != nil {
				return nil, err
			}

			if value != nil {
				return value, nil
			}
		}

		k = (k + 1) % ref.length
	}

	return nil, nil
}

func (r *readerImpl) calcHash(key []byte) uint32 {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.hash.Write(key)
	return r.hash.Sum32()
}

func (r *readerImpl) readValue(entry slot, key []byte) ([]byte, error) {
	var (
		keySize, valSize uint32
		givenKeySize uint32 = uint32(len(key))
	)

	err := r.readPair(entry.position, &keySize, &valSize)
	if err != nil {
		return nil, err
	}

	if keySize != givenKeySize {
		return nil, nil
	}

	data := make([]byte, keySize + valSize)
	_, err = r.reader.ReadAt(data, int64(entry.position + 8))

	if err != nil {
		return nil, err
	}

	if bytes.Compare(data[:keySize], key) != 0 {
		return nil, nil
	}

	return data[keySize:], err
}

func (r *readerImpl) readPair(pos uint32, a, b *uint32) error {
	pair := make([]byte, 8, 8)

	_, err := r.reader.ReadAt(pair, int64(pos))
	if err != nil {
		return err
	}

	*a, *b = binary.LittleEndian.Uint32(pair), binary.LittleEndian.Uint32(pair[4:])
	return nil
}
