// dump.go reads a constant database from input file and prints the database contents in csv format to stdout

package main

import (
	"encoding/csv"
	"github.com/alldroll/cdb"
	"log"
	"os"
)

func main() {
	var sourceFile *os.File

	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s source", os.Args[0])
	}

	sourceFile, err := os.OpenFile(os.Args[1], os.O_RDONLY, 0)
	if err != nil {
		log.Fatalf("[Fail to open source file] %s", err)
	}

	defer sourceFile.Close()

	cdbHandle := cdb.New()
	cdbReader, err := cdbHandle.GetReader(sourceFile)
	if err != nil {
		log.Fatal(err)
	}

	iterator, err := cdbReader.Iterator()
	if err != nil {
		log.Fatal(err)
	}

	csvWriter := csv.NewWriter(os.Stdout)
	defer csvWriter.Flush()

	for {
		record := iterator.Record()

		keyReader, keySize := record.Key()
		key := make([]byte, keySize)
		if _, err = keyReader.Read(key); err != nil {
			log.Fatal(err)
		}

		valReader, valSize := record.Value()
		value := make([]byte, valSize)
		if _, err = valReader.Read(value); err != nil {
			log.Fatal(err)
		}

		err = csvWriter.Write([]string{string(key), string(value)})
		if err != nil {
			log.Fatal(err)
		}

		ok, err := iterator.Next()
		if err != nil {
			log.Fatal(err)
		}

		if !ok {
			break
		}
	}
}
