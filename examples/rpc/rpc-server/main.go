package main

import (
	"log"
	"net/http"

	"github.com/jcelliot/turnpike"
)

var client *turnpike.Client

func main() {
	//log.Println("Starting Node.")
	turnpike.Debug()

	s := turnpike.NewBasicWebsocketServer("crossbardemo")

	server := &http.Server{
		Handler: s,
		Addr:    ":8000",
	}

	//log.Println("turnpike server starting on port 8000")
	log.Fatal(server.ListenAndServe())
}
