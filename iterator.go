package cdb

type iteratorImpl struct {
	position, endPosition uint32
	r *readerImpl
	key, value []byte
}

func (i *iteratorImpl) Next() (bool, error) {
	if i.IsDereferencable() {
		i.key, i.value = nil, nil
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

func (i *iteratorImpl) Value() []byte {
	return i.value
}

func (i *iteratorImpl) Key() []byte {
	return i.key
}

func (i *iteratorImpl) IsDereferencable() bool {
	return i.position >= i.endPosition
}
