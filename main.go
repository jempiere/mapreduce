package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	fmt.Printf("Hello, World!\n")
	var err = splitDatabase("files/austen.db", []string{
		"files/austen-0.db", "files/austen-1.db", "files/austen-2.db",
		"files/austen-3.db", "files/austen-4.db", "files/austen-5.db",
		"files/austen-6.db", "files/austen-7.db", "files/austen-8.db"})
	fmt.Printf("lgima\n")
	fmt.Printf("ERROR: %v\n", err)
	// localhost()
}

func localhost() {
	address := "localhost:8080"
	tempdir := "./files"

	go func() {
		http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
		if err := http.ListenAndServe(address, nil); err != nil {
			log.Printf("Err in HTTP Server for %s: %v", address, err)
		}
	}()
}
