package cdb

import (
	"io"
	"encoding/binary"
	"hash"
	"log"
)

// writerImpl
type writerImpl struct {
	tables [TABLE_NUM]hashTable
	writer io.WriteSeeker
	hash hash.Hash32
	begin, current int64
}

//
func newWriter(writer io.WriteSeeker, h hash.Hash32, startPosition int64) *writerImpl {
	begin, _ := writer.Seek(0, io.SeekCurrent)
	writer.Seek(startPosition, io.SeekStart)

	log.Println("START", startPosition)

	return &writerImpl{
		writer: writer,
		hash: h,
		begin: begin,
		current: startPosition,
	}
}

//
func (w *writerImpl) Put(key string, keySize uint32, value string, valueSize uint32) error {
	binary.Write(w.writer, binary.LittleEndian, keySize)
	binary.Write(w.writer, binary.LittleEndian, []byte(key))

	binary.Write(w.writer, binary.LittleEndian, valueSize)
	binary.Write(w.writer, binary.LittleEndian, []byte(value))

	w.hash.Write([]byte(key))
	h := w.hash.Sum32()

	table := w.tables[h % TABLE_NUM]
	table = append(table, slot{h, uint32(w.current)})
	w.tables[h % TABLE_NUM] = table

	w.current += int64(4 + keySize + 4 + valueSize)

	return nil
}

//
func (w *writerImpl) Close() error {
	for _, table := range w.tables {
		n := uint32(len(table) << 1)
		if n == 0 {
			continue
		}

		slots := make(hashTable, n)

		for _, slot := range table {
			k := (slot.hash >> 8) % n

			// linear ...
			for slots[k].position != 0 {
				k = (k + 1) % n
			}

			slots[k].position = slot.position
			slots[k].hash = slot.hash
		}

		log.Println("slots", slots)

		for _, slot := range slots {
			pos, err := w.writer.Seek(0, io.SeekCurrent)
			log.Println("1", pos, err, slot)
			binary.Write(w.writer, binary.LittleEndian, slot.hash)
			binary.Write(w.writer, binary.LittleEndian, slot.position)

			pos, err = w.writer.Seek(0, io.SeekCurrent)
			log.Println("2", pos, err)
		}
	}

	offset, _ := w.writer.Seek(0, io.SeekCurrent)
	w.writer.Seek(w.begin, io.SeekStart)

	for _, table := range w.tables {
		n := uint32(len(table) << 1)
		if n == 0 {
			binary.Write(w.writer, binary.LittleEndian, n)
		} else {
			binary.Write(w.writer, binary.LittleEndian, uint32(w.current))
		}


		binary.Write(w.writer, binary.LittleEndian, n)

		/*
		pos, _ := w.writer.Seek(0, io.SeekCurrent)
		log.Println(w.current, n, pos)
		*/

		w.current += int64(4 * n * 2)
	}

	w.writer.Seek(offset, io.SeekStart)
	return nil
}
