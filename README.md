## CDB golang implementation

cdb is a fast, reliable, simple package for creating and reading constant databases

see [docs](http://cr.yp.to/cdb.html) for more details


Usage
-----

TODO add usage

Example
-------

```go
f, _ := os.Create("test.cdb")
defer f.Close()

handle := cdb.New()

data := []struct {
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

writer, err := handle.GetWriter(f)
if err != nil {
    t.Error(err)
}

for _, c := range data {
    writer.Put([]byte(c.key), []byte(c.value))
}

writer.Close()
reader, _ := handle.GetReader(f)

for _, c := range data {
    value, err := reader.Get([]byte(c.key))
}
```
