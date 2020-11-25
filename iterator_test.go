package cdb

import (
	"os"
	"strconv"
	"testing"
)

func (suite *CDBTestSuite) getCDBIterator() (Iterator, error) {
	reader := suite.getCDBReader()
	return reader.Iterator()
}

func (suite *CDBTestSuite) mustGetCDBIterator() Iterator {
	iterator, err := suite.getCDBIterator()
	suite.Require().Nilf(err, "Iterator creation error: %#v", err)
	return iterator
}

func (suite *CDBTestSuite) TestIteratorOnEmptyDataSet() {
	suite.writeEmptyCDB()

	iterator, err := suite.getCDBIterator()

	suite.EqualError(err, ErrEmptyCDB.Error())

	suite.Nilf(iterator, "Iterator must return false on HasNext")
}

func (suite *CDBTestSuite) EqualKeyValue(iter Iterator, testRec testCDBRecord) {
	suite.EqualRecords(iter.Record(), testRec)

	key, err := iter.Key()
	suite.Nilf(err, "Cant get key: %#v", err)
	value, err := iter.Value()
	suite.Nilf(err, "Cant get value: %#v", err)

	suite.Equal(testRec.key, key, "Keys must be equal. Got: %s, Expected: %s", key, testRec.key)
	suite.Equal(testRec.val, value, "Values must be equal. Got: %s, Expected: %s", value, testRec.val)
}

func (suite *CDBTestSuite) EqualRecords(record Record, testRec testCDBRecord) {

	keyReader, keySize := record.Key()
	key := make([]byte, int(keySize))
	readSize, err := keyReader.Read(key)
	suite.Equal(readSize, int(keySize))
	suite.Nilf(err, "Eror on reading key %s: %#v", string(key), err)

	valReader, valSize := record.Value()
	value := make([]byte, int(valSize))
	readSize, err = valReader.Read(value)
	suite.Equal(readSize, int(valSize))
	suite.Nilf(err, "Eror on reading value %s: %#v", string(value), err)

	suite.Equal(testRec.key, key, "Keys must be equal. Got: %s, Expected: %s", key, testRec.key)
	suite.Equal(testRec.val, value, "Values must be equal. Got: %s, Expected: %s", value, testRec.val)
}

func (suite *CDBTestSuite) TestIterator() {
	suite.fillTestCDB()

	iterator := suite.mustGetCDBIterator()

	for i, testRec := range suite.testRecords {
		suite.EqualKeyValue(iterator, testRec)

		ok, err := iterator.Next()
		suite.Nilf(err, "Error on interator.Next: %#v", err)

		if i == len(suite.testRecords)-1 {
			suite.False(ok, "Iterator must return False on last record")
		} else {
			suite.True(ok, "Iterator has not enough records")
		}
	}

	ok := iterator.HasNext()
	suite.False(ok, "HasNext should returns false if has no more records")
}

func (suite *CDBTestSuite) TestIteratorNext() {
	suite.fillTestCDB()
	iterator := suite.mustGetCDBIterator()

	record1 := iterator.Record()
	suite.EqualRecords(record1, suite.testRecords[0])

	iterator.Next()
	record2 := iterator.Record()
	suite.EqualRecords(record2, suite.testRecords[1])

	suite.NotEqual(record1, record2)
}

func (suite *CDBTestSuite) TestIteratorAt() {
	suite.fillTestCDB()
	reader := suite.getCDBReader()

	for _, testRec := range suite.testRecords {
		iterator, err := reader.IteratorAt(testRec.key)
		suite.Nilf(err, "Unexpected error for reader.IteratorAt: %#v", err)

		suite.EqualKeyValue(iterator, testRec)
	}
}

func BenchmarkIteratorAt(b *testing.B) {

	n := 1000
	f, _ := os.Create("test.cdb")
	defer f.Close()
	defer os.Remove("test.cdb")

	handle := New()
	writer, _ := handle.GetWriter(f)

	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(strconv.Itoa(i))
		writer.Put(keys[i], keys[i])
	}

	writer.Close()
	reader, _ := handle.GetReader(f)

	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		reader.IteratorAt(keys[j%n])
	}
}

func BenchmarkIteratorNext(b *testing.B) {
	iter, f := getTestCDBIterator(b)
	defer f.Close()
	defer os.Remove(f.Name())

	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		iter.Next()
	}
	b.StopTimer()

}

func BenchmarkIteratorRecordKey(b *testing.B) {
	iter, f := getTestCDBIterator(b)
	defer f.Close()
	defer os.Remove(f.Name())

	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		reader, size := iter.Record().Key()
		key := make([]byte, size)
		reader.Read(key)
	}
	b.StopTimer()

}

func BenchmarkIteratorKey(b *testing.B) {
	iter, f := getTestCDBIterator(b)
	defer f.Close()
	defer os.Remove(f.Name())

	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		iter.Key()
	}
	b.StopTimer()
}

func getTestCDBIterator(b *testing.B) (Iterator, *os.File) {
	f, err := os.Create("test.cdb")
	if err != nil {
		panic(err)
	}

	handle := New()
	writer, err := handle.GetWriter(f)
	if err != nil {
		panic(err)
	}

	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(strconv.Itoa(i))
		writer.Put(keys[i], keys[i])
	}

	writer.Close()
	reader, err := handle.GetReader(f)
	if err != nil {
		panic(err)
	}

	iter, err := reader.Iterator()
	if err != nil {
		panic(err)
	}

	return iter, f
}
