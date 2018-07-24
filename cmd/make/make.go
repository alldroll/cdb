// make.go reads a series of csv encoded records from input file (source) and writes a constant database to output file (destination)

package main

import (
	"bufio"
	"encoding/csv"
	"github.com/alldroll/cdb"
	"io"
	"log"
	"os"
)

func main() {
	var (
		sourceFile      *os.File
		destinationFile *os.File
		err             error
	)

	if len(os.Args) != 3 {
		log.Fatalf("Usage: %s source destination", os.Args[0])
	}

	sourceFile, err = os.OpenFile(os.Args[1], os.O_RDONLY, 0)
	if err != nil {
		log.Fatalf("[Fail to open source file] %s", err)
	}

	defer sourceFile.Close()

	destinationFile, err = os.OpenFile(os.Args[2], os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("[Fail to open destination file] %s", err)
	}

	defer destinationFile.Close()

	cdbHandle := cdb.New()
	cdbWriter, err := cdbHandle.GetWriter(destinationFile)
	if err != nil {
		log.Fatal(err)
	}

	r := csv.NewReader(bufio.NewReader(sourceFile))
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}

		if len(record) != 2 {
			log.Fatal("Invalid csv format")
		}

		key, value := record[0], record[1]
		err = cdbWriter.Put([]byte(key), []byte(value))
		if err != nil {
			log.Fatal(err)
		}
	}

	err = cdbWriter.Close()
	if err != nil {
		log.Fatal(err)
	}
}
