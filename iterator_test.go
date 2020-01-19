package cdb

import (
	"os"
	"strconv"
	"testing"
)

func (suite *CDBTestSuite) getCDBIterator() Iterator {
	reader := suite.getCDBReader()
	iterator, err := reader.Iterator()
	suite.Require().Nilf(err, "Iterator creation error: %#v", err)
	return iterator
}

func (suite *CDBTestSuite) TestIteratorOnEmptyDataSet() {
	suite.writeEmptyCDB()

	iterator := suite.getCDBIterator()

	suite.False(iterator.HasNext(), "Iterator must return false on HasNext")

	ok, err := iterator.Next()
	suite.False(ok, "Next should returns false")
	suite.Nilf(err, "Got unexpected error %v", err)
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

	iterator := suite.getCDBIterator()

	for i, testRec := range suite.testRecords {
		record := iterator.Record()
		suite.EqualRecords(record, testRec)

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

func (suite *CDBTestSuite) TestIteratorAt() {
	suite.fillTestCDB()
	reader := suite.getCDBReader()

	for _, testRec := range suite.testRecords {
		iterator, err := reader.IteratorAt(testRec.key)
		suite.Nilf(err, "Unexpected error for reader.IteratorAt: %#v", err)

		record := iterator.Record()
		suite.EqualRecords(record, testRec)
	}
}

func BenchmarkReaderIteratorAt(b *testing.B) {
	b.StopTimer()

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
	b.StartTimer()

	reader, _ := handle.GetReader(f)

	for j := 0; j < b.N; j++ {
		reader.IteratorAt(keys[j%n])
	}
}
