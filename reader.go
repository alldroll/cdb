package cdb

import (
	"io"
	"hash"
	"encoding/binary"
	"bytes"
)

// hashTableRef is a pointer that state a position and a length of the hash table
// position is the starting byte position of the hash table.
// The length is the number of slots in the hash table.
type hashTableRef struct {
	position, length uint32
}

type readerImpl struct {
	refs [TABLE_NUM]hashTableRef
	reader io.ReadSeeker
	hash hash.Hash32
}

func newReader(reader io.ReadSeeker, h hash.Hash32) (*readerImpl, error) {
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
	for i, _ := range r.refs {
		err := binary.Read(r.reader, binary.LittleEndian, &r.refs[i].position)
		if err != nil {
			return err
		}

		err = binary.Read(r.reader, binary.LittleEndian, &r.refs[i].length)
		if err != nil {
			return err
		}
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
	r.hash.Write(key)
	h := r.hash.Sum32()
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
		r.reader.Seek(int64(ref.position + k * slotSize), io.SeekStart)

		binary.Read(r.reader, binary.LittleEndian, &entry.hash)
		binary.Read(r.reader, binary.LittleEndian, &entry.position)

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

func (r *readerImpl) readValue(entry slot, key []byte) ([]byte, error) {
	var (
		valueSize, keySize uint32
		givenKeySize uint32 = uint32(len(key))
	)

	pos := entry.position
	r.reader.Seek(int64(pos), io.SeekStart)

	err := binary.Read(r.reader, binary.LittleEndian, &keySize)
	if err != nil {
		return nil, err
	}

	if keySize != givenKeySize {
		return nil, nil
	}

	recordKey := make([]byte, givenKeySize)
	err = binary.Read(r.reader, binary.LittleEndian, &recordKey)
	if err != nil {
		return nil, err
	}

	if bytes.Compare(recordKey, key) != 0 {
		return nil, nil
	}

	err = binary.Read(r.reader, binary.LittleEndian, &valueSize)
	if err != nil {
		return nil, err
	}

	recordValue := make([]byte, valueSize)
	err = binary.Read(r.reader, binary.LittleEndian, recordValue)
	if err != nil {
		return nil, err
	}

	return recordValue, err
}
