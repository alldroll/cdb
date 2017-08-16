package cdb

import (
	"testing"
	"io"
	"fmt"
	"os"
)

func TestUsage(t *testing.T) {

	os.Create("test.cdb")

	f, err := os.OpenFile("test.cdb", os.O_RDWR, 0600)
	defer f.Close()

	fmt.Println(f, err)

	cdb := New(&hashImpl{})
	writer := cdb.GetWriter(f)

	writer.Put([]byte("test1"),  []byte("value1" ))
	writer.Put([]byte("test2"), []byte("value2"))

	writer.Close()

	f.Seek(0, io.SeekStart)
	reader, _ := cdb.GetReader(f)
	value, err := reader.Get([]byte("test2"))

	fmt.Println(string(value), err)
}
