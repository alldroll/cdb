package cdb

import (
	"testing"
	"io"
	"fmt"
	"os"
)

func TestWriterImpl_Put(t *testing.T) {
	f, err := os.OpenFile("test.cdb", os.O_RDWR, 0600)

	fmt.Println(f, err)


	cdb := New(&hashImpl{})
	writer := cdb.GetWriter(f)

	writer.Put("test1", uint32(len("test1")), "value1", uint32(len("value1")))
	writer.Put("test2", uint32(len("test2")), "value2", uint32(len("value2")))


	writer.Close()

	f.Seek(0, io.SeekStart)
	reader := cdb.NewReader(f)
	value, err := reader.Get("test2")

	fmt.Println(value, err)

	f.Close()
}
