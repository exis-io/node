package main

import (
	"log"
	"net/http"
	"os"

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

	certFile := os.Getenv("EXIS_CERT")
	keyFile := os.Getenv("EXIS_KEY")

	if certFile != "" && keyFile != "" {
		log.Fatal(server.ListenAndServeTLS(certFile, keyFile))
	} else {
		log.Fatal(server.ListenAndServe())
	}
}
