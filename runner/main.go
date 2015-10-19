package main

import (
	"flag"
	"github.com/ParadropLabs/node"
	"log"
	"net/http"
	"os"
)

// var client *rabric.Client

var config node.NodeConfig

func main() {
	node.Log()

	//
	var configPath = flag.String("configpath", "config.json", "the configuration file for the node")
	flag.Parse()
	config, _ := node.LoadConfig(*configPath)

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
