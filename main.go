package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
)

func main2() {
	var err error
	var tempdir = "./tmp"
	fmt.Printf("Ligma\n")
	localhost()
	var splits = []string{
		"files/austen-0.db", "files/austen-1.db", "files/austen-2.db",
		"files/austen-3.db", "files/austen-4.db", "files/austen-5.db",
		"files/austen-6.db", "files/austen-7.db", "files/austen-8.db"}
	var urls = []string{}
	for _, str := range splits {
		urls = append(urls, "http://localhost:8080/data/"+strings.TrimPrefix(str, "files/"))
	}
	err = splitDatabase("files/austen.db", splits)
	if err != nil {
		log.Fatalf("Jayson needs to convince me to use font ligatures.")
	}
	_, err = mergeDatabases(urls, "files/austen_merged.db", tempdir)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}
}

func localhost() {
	address := "localhost:8080"
	tempdir := "./files"

	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))

	// THIS CODE WAS AUTO-GENERATED BY JAROD

	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Not Jarod's fault")
	}
	go func() {
		if err := http.Serve(listener, nil); err != nil {
			log.Fatalf("LOSER")
		}
	}()
}

// bogos binted


