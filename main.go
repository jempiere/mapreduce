package mapreduce

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	fmt.Printf("Hello, World!\n")
	localhost()
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

// bogos binted
