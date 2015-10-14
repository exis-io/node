package main

import (
	"log"
	"net/http"
	"os"

	"github.com/ParadropLabs/node"
)

// var client *rabric.Client

var config node.NodeConfig

func main() {
	node.Log()

	// TODO: configurable location for config file.
	config, _ := node.LoadConfig("config.json")

	// Pass certificate here
	s := node.CreateNode(config)

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
