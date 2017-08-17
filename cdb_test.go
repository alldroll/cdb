package cdb

import (
	"testing"
	"io"
	"os"
	"strconv"
)

func TestShouldReturnAllValues(t *testing.T) {
	f, _ := os.Create("test.cdb")
	defer f.Close()

	handle := New(&hashImpl{})

	cases := []struct{
		key, value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
		{"key6", "value6"},
		{"key7", "value7"},
	}

	writer := handle.GetWriter(f)
	for _, c := range cases {
		writer.Put([]byte(c.key), []byte(c.value))
	}

	writer.Close()
	f.Seek(0, io.SeekStart)
	reader, _ := handle.GetReader(f)

	for _, c := range cases {
		value, err := reader.Get([]byte(c.key))
		if err != nil {
			t.Error(err)
		}

		if string(value) != c.value {
			t.Errorf("Expected value %s, got %s", c.value, value)
		}
	}
}

func TestShouldReturnNilOnNonExistingKeys(t *testing.T) {
	f, _ := os.Create("test.cdb")
	defer f.Close()
	defer os.Remove("test.cdb")

	handle := New(&hashImpl{})

	cases := []struct{
		key, value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
		{"key6", "value6"},
		{"key7", "value7"},
	}

	writer := handle.GetWriter(f)
	for _, c := range cases {
		writer.Put([]byte(c.key), []byte(c.value))
	}

	writer.Close()
	f.Seek(0, io.SeekStart)
	reader, _ := handle.GetReader(f)

	keys := [...]string{
		"nkey1",
		"nkey2",
		"nkey3",
		"nkey4",
		"nkey5",
		"nkey6",
		"nkey7",
	}

	for _, key := range keys {
		value, err := reader.Get([]byte(key))
		if err != nil {
			t.Error(err)
		}

		if value != nil {
			t.Errorf("Expected nil, got %s", value)
		}
	}
}

func BenchmarkReaderImpl_Get(b *testing.B) {
	b.StopTimer()

	n := 1000
	f, _ := os.Create("test.cdb")
	defer f.Close()
	defer os.Remove("test.cdb")

	handle := New(&hashImpl{})
	writer := handle.GetWriter(f)

	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(strconv.Itoa(i))
		writer.Put(keys[i], keys[i])
	}

	writer.Close()
	f.Seek(0, io.SeekStart)

	b.StartTimer()

	reader, _ := handle.GetReader(f)

	for j := 0; j < b.N; j++ {
		i := j % n
		reader.Get(keys[i])
	}
}

func BenchmarkWriterImpl_Put(b *testing.B) {
	f, _ := os.Create("test.cdb")
	defer f.Close()
	defer os.Remove("test.cdb")

	handle := New(&hashImpl{})
	writer := handle.GetWriter(f)

	for j := 0; j < b.N; j++ {
		key := []byte(strconv.Itoa(j))
		writer.Put(key, key)
	}

	writer.Close()
}
