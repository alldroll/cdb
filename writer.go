package cdb

import (
	"bufio"
	"encoding/binary"
	"io"
)

// slot (bucket)
type slot struct {
	hash, position uint32
}

// hashTable is a linearly probed initialize hash table
type hashTable []slot

// writerImpl implements Writer interface
type writerImpl struct {
	tables         [tableNum]hashTable
	writer         io.WriteSeeker
	buffer         *bufio.Writer
	hasher         Hasher
	begin, current int64
}

// newWriter returns pointer to new instance of writerImpl
func newWriter(writer io.WriteSeeker, hasher Hasher) (*writerImpl, error) {
	startPosition := int64(tablesRefsSize)
	begin, err := writer.Seek(0, io.SeekCurrent)

	if err != nil {
		return nil, err
	}

	if _, err = writer.Seek(startPosition, io.SeekStart); err != nil {
		return nil, err
	}

	return &writerImpl{
		writer:  writer,
		buffer:  bufio.NewWriter(writer),
		hasher:  hasher,
		begin:   begin,
		current: startPosition,
	}, nil
}

// Put saves a new associated pair <key, value> into databases. Returns an error on failure.
func (w *writerImpl) Put(key, value []byte) error {
	lenKey, lenValue := len(key), len(value)

	if uint64(lenKey) > maxUint || uint64(lenValue) > maxUint {
		return ErrOutOfMemory
	}

	if err := writePair(w.buffer, uint32(lenKey), uint32(lenValue)); err != nil {
		return err
	}

	if err := binary.Write(w.buffer, binary.LittleEndian, key); err != nil {
		return err
	}

	if err := binary.Write(w.buffer, binary.LittleEndian, value); err != nil {
		return err
	}

	hashFunc := w.hasher()
	hashFunc.Write(key)
	h := hashFunc.Sum32()

	table := w.tables[h%tableNum]
	table = append(table, slot{h, uint32(w.current)})
	w.tables[h%tableNum] = table

	if err := w.addPos(8); err != nil {
		return err
	}

	if err := w.addPos(lenKey); err != nil {
		return err
	}

	if err := w.addPos(lenValue); err != nil {
		return err
	}

	return nil
}

// Close commits database, makes it possible for reading.
func (w *writerImpl) Close() error {
	w.buffer.Flush()

	for _, table := range &w.tables {
		n := uint32(len(table) << 1)
		if n == 0 {
			continue
		}

		slots := make(hashTable, n)

		for _, slot := range table {
			k := (slot.hash >> 8) % n

			// Linear probing
			for slots[k].position != 0 {
				k = (k + 1) % n
			}

			slots[k].position = slot.position
			slots[k].hash = slot.hash
		}

		for _, slot := range slots {
			if err := writePair(w.writer, slot.hash, slot.position); err != nil {
				return err
			}
		}
	}

	offset, err := w.writer.Seek(0, io.SeekCurrent)

	if err != nil {
		return err
	}

	if _, err := w.writer.Seek(w.begin, io.SeekStart); err != nil {
		return err
	}

	var pos uint32

	for _, table := range &w.tables {
		n := len(table) << 1

		if n == 0 {
			pos = 0
		} else {
			pos = uint32(w.current)
		}

		if err := writePair(w.writer, pos, uint32(n)); err != nil {
			return err
		}

		if err := w.addPos(slotSize * n); err != nil {
			return err
		}
	}

	if _, err := w.writer.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	return nil
}

// addPos try to shift current position on len. Returns err when was attempt to create a database up to 4 gb
func (w *writerImpl) addPos(offset int) error {
	newPos := w.current + int64(offset)

	if newPos >= maxUint {
		return ErrOutOfMemory
	}

	w.current = newPos

	return nil
}

// writePair writes binary representation of two uint32 numbers to io.Writer
func writePair(writer io.Writer, a, b uint32) error {
	var pairBuf = []uint32{a, b}

	return binary.Write(writer, binary.LittleEndian, pairBuf)
}
