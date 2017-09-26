package cdb

import (
	"io"
	"encoding/binary"
	"hash"
	"unsafe"
	"bufio"
)

// slot (bucket) is ..
type slot struct {
	hash, position uint32
}

// hashTable is a linearly probed open hash table
type hashTable []slot

// writerImpl
type writerImpl struct {
	tables [TABLE_NUM]hashTable
	writer io.WriteSeeker
	buffer *bufio.Writer
	hash hash.Hash32
	begin, current int64
}

//
func newWriter(writer io.WriteSeeker, h hash.Hash32) (*writerImpl, error) {
	startPosition := int64(calcTablesRefsSize())
	begin, err := writer.Seek(0, io.SeekCurrent)

	if err != nil {
		return nil, err
	}

	_, err = writer.Seek(startPosition, io.SeekStart)
	if err != nil {
		return nil, err
	}

	return &writerImpl{
		writer: writer,
		buffer: bufio.NewWriter(writer),
		hash: h,
		begin: begin,
		current: startPosition,
	}, nil
}

//
func (w *writerImpl) Put(key []byte, value []byte) error {
	err := writePair(w.buffer, uint32(len(key)), uint32(len(value)))
	if err != nil {
		return err
	}

	err = binary.Write(w.buffer, binary.LittleEndian, key)
	if err != nil {
		return err
	}

	err = binary.Write(w.buffer, binary.LittleEndian, value)
	if err != nil {
		return err
	}

	w.hash.Write(key)
	h := w.hash.Sum32()

	table := w.tables[h % TABLE_NUM]
	table = append(table, slot{h, uint32(w.current)})
	w.tables[h % TABLE_NUM] = table

	w.current += int64(4 + len(key) + 4 + len(value))

	return nil
}

//
func (w *writerImpl) Close() error {
	w.buffer.Flush()

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

		for _, slot := range slots {
			err := writePair(w.writer, slot.hash, slot.position)
			if err != nil {
				return err
			}
		}
	}

	offset, _ := w.writer.Seek(0, io.SeekCurrent)
	w.writer.Seek(w.begin, io.SeekStart)

	var pos uint32 = 0

	slotSize := calcSlotSize()
	for _, table := range w.tables {
		n := uint32(len(table) << 1)
		if n == 0 {
			pos = 0
		} else {
			pos = uint32(w.current)
		}

		err := writePair(w.writer, pos, n)
		if err != nil {
			return err
		}

		w.current += int64(slotSize * n)
	}

	w.writer.Seek(offset, io.SeekStart)
	return nil
}

// calcTablesRefsSize
func calcTablesRefsSize() uint32 {
	return uint32(unsafe.Sizeof(hashTableRef{}) * TABLE_NUM)
}

func calcSlotSize() uint32 {
	return uint32(unsafe.Sizeof(slot{}))
}

func writePair(writer io.Writer, a, b uint32) error {
	var pairBuf = []uint32 {a, b}
	return binary.Write(writer, binary.LittleEndian, pairBuf)
}
