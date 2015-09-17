package main

import (
	"log"
	"net/http"

	"github.com/ParadropLabs/node"
)

// var client *rabric.Client

func main() {
	node.Log()

	// Pass certificate here
	s := node.CreateNode("pd.routers.aardvark")

	server := &http.Server{
		Handler: s,
		Addr:    ":8000",
	}

	log.Fatal(server.ListenAndServe())
}
