package cdb

import (
	"io"
	"os"
	"strconv"
	"testing"
)

func TestIteratorOnEmptyDataSet(t *testing.T) {
	f, _ := os.Create("test.cdb")
	defer f.Close()
	defer os.Remove("test.cdb")

	handle := New()

	writer, err := handle.GetWriter(f)
	if err != nil {
		t.Error(err)
	}

	writer.Close()
	reader, _ := handle.GetReader(f)

	iterator, err := reader.Iterator()
	if err != nil {
		t.Errorf("Got unexpected error %v", err)
	}

	if iterator.HasNext() {
		t.Errorf("Iterator should not be deferencable")
	}

	ok, err := iterator.Next()
	if ok {
		t.Errorf("Next should returns true")
	}

	if err != nil {
		t.Errorf("Got unexpected error %v", err)
	}
}

func TestIterator(t *testing.T) {
	f, _ := os.Create("test.cdb")
	defer f.Close()
	defer os.Remove("test.cdb")

	handle := New()

	cases := []struct {
		key, value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
		{"key6", "value61"},
		{"key6", "value62"},
		{"key7", "value7"},
	}

	writer, err := handle.GetWriter(f)
	if err != nil {
		t.Error(err)
	}

	for _, c := range cases {
		writer.Put([]byte(c.key), []byte(c.value))
	}

	writer.Close()
	reader, _ := handle.GetReader(f)

	iterator, err := reader.Iterator()
	if err != nil {
		t.Errorf("Got unexpected error %s", err)
	}

	for _, c := range cases {
		record := iterator.Record()

		keyReader, keySize := record.Key()
		key := make([]byte, int(keySize))
		if _, err = keyReader.Read(key); err != nil {
			t.Error(err)
		}

		valReader, valSize := record.Value()
		value := make([]byte, int(valSize))
		if _, err = valReader.Read(value); err != nil {
			t.Error(err)
		}

		if c.key != string(key) || c.value != string(value) {
			t.Errorf(
				"Expected key to be %s, value to be %s, got %s, %s",
				c.key,
				c.value,
				key,
				value,
			)
		}

		_, err := iterator.Next()
		if err != nil {
			t.Errorf("Got unexpected error %s", err)
		}
	}

	ok, err := iterator.Next()
	if ok {
		t.Errorf("Next should return false")
	}

	if err != nil {
		t.Errorf("Got unexpected error %s", err)
	}
}

func TestIteratorAt(t *testing.T) {
	f, _ := os.Create("test.cdb")
	defer f.Close()
	defer os.Remove("test.cdb")

	handle := New()

	cases := []struct {
		key, value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
		{"key6", "value6"},
	}

	writer, err := handle.GetWriter(f)
	if err != nil {
		t.Error(err)
	}

	for _, c := range cases {
		writer.Put([]byte(c.key), []byte(c.value))
	}

	writer.Close()
	reader, _ := handle.GetReader(f)

	for _, c := range cases {
		iterator, err := reader.IteratorAt([]byte(c.key))

		if err != nil {
			t.Errorf("Got unexpected error %s", err)
		}

		record := iterator.Record()

		var (
			keyReader, valReader io.Reader
			keySize, valSize     uint32
			key, value           []byte
		)

		for i := 0; i < 10; i++ {
			keyReader, keySize = record.Key()
			key = make([]byte, int(keySize))

			if _, err = keyReader.Read(key); err != nil {
				t.Error(err)
			}

			valReader, valSize = record.Value()
			value = make([]byte, int(valSize))

			if _, err = valReader.Read(value); err != nil {
				t.Error(err)
			}
		}

		if c.key != string(key) || c.value != string(value) {
			t.Errorf(
				"Expected key to be %s, value to be %s, got %s, %s",
				c.key,
				c.value,
				key,
				value,
			)
		}
	}
}

func BenchmarkReaderIteratorAt(b *testing.B) {

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
	b.ResetTimer()

	reader, _ := handle.GetReader(f)

	for j := 0; j < b.N; j++ {
		reader.IteratorAt(keys[j%n])
	}
}
