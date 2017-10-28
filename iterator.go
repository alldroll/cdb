package cdb

// iteratorImpl represents implementation of Iterator
type iteratorImpl struct {
	position uint32
	r *readerImpl
	key, value []byte
	hasNext bool
}

// Next moves iterator to the next record. Returns true on success otherwise false
func (i *iteratorImpl) Next() (bool, error) {
	if i.position >= i.r.endPos {
		i.key, i.value = nil, nil
		i.hasNext = false
		return false, nil
	}

	var keySize, valSize uint32

	err := i.r.readPair(i.position, &keySize, &valSize)
	if err != nil {
		return false, err
	}

	data := make([]byte, keySize + valSize)
	_, err = i.r.reader.ReadAt(data, int64(i.position + 8))
	if err != nil {
		return false, err
	}

	i.key, i.value = data[:keySize], data[keySize:]
	i.position += keySize + valSize + 8

	return true, nil
}

// Value returns value of current record. Returns nil if iterator is not valid
func (i *iteratorImpl) Value() []byte {
	return i.value
}

// Key returns key of current record. Returns nil if iterator is not valid
func (i *iteratorImpl) Key() []byte {
	return i.key
}

// IsDereferencable detects is Valid Iterator
func (i *iteratorImpl) HasNext() bool {
	return i.hasNext
}
