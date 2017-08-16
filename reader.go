package cdb

import (
	"io"
	"hash"
	"encoding/binary"
)

// hashTableRef
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

	for j = 0; j < ref.length; j++ {
		r.reader.Seek(int64(ref.position + k * 8), io.SeekStart)

		binary.Read(r.reader, binary.LittleEndian, &entry.hash)
		binary.Read(r.reader, binary.LittleEndian, &entry.position)

		if entry.hash == h {
			return r.readValue(entry, key)
		}

		k = (k + 1) % ref.length
	}

	return nil, nil
}

func (r *readerImpl) readValue(entry slot, key []byte) ([]byte, error) {
	var valueSize uint32

	pos := entry.position + uint32(len(key)) + 4
	r.reader.Seek(int64(pos), io.SeekStart)

	err := binary.Read(r.reader, binary.LittleEndian, &valueSize)
	if err != nil {
		return nil, err
	}

	data := make([]byte, valueSize)
	err = binary.Read(r.reader, binary.LittleEndian, data)
	if err != nil {
		return nil, err
	}

	return data, err
}
