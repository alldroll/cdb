package cdb

import (
	"io"
	"hash"
	"encoding/binary"
	"log"
)

type readerImpl struct {
	refs [TABLE_NUM]hashTableRef
	reader io.ReadSeeker
	hash hash.Hash32
}

func newReader(reader io.ReadSeeker, h hash.Hash32) *readerImpl {
	r := &readerImpl{
		reader: reader,
		hash: h,
	}

	r.open()
	return r
}

func (r *readerImpl) open() error {
	for i, _ := range r.refs {
		binary.Read(r.reader, binary.LittleEndian, &r.refs[i].position)
		binary.Read(r.reader, binary.LittleEndian, &r.refs[i].length)
	}

	log.Println("READER")

	return nil
}

func (r *readerImpl) Get(key string) (string, error) {
	r.hash.Write([]byte(key))
	h := r.hash.Sum32()
	ref := r.refs[h % TABLE_NUM]

	if ref.length == 0 {
		return "", nil
	}

	k := (h >> 8) % ref.length
	r.reader.Seek(int64(ref.position + k * 8), io.SeekStart)

	var (
		entry slot
		j uint32
	)

	for j = 0; j < ref.length; j++ {
		binary.Read(r.reader, binary.LittleEndian, &entry.hash)
		binary.Read(r.reader, binary.LittleEndian, &entry.position)

		log.Println(entry.hash, entry.position, h)

		if entry.hash != h {
			k = (k + 1) % ref.length
			r.reader.Seek(int64(ref.position + k * 8), io.SeekStart)
			continue
		}

		pos := entry.position + uint32(len(key)) + 4
		r.reader.Seek(int64(pos), io.SeekStart)

		var valueSize uint32
		binary.Read(r.reader, binary.LittleEndian, &valueSize)
		data := make([]byte, valueSize)
		binary.Read(r.reader, binary.LittleEndian, data)

		return string(data), nil
	}

	return "", nil
}
