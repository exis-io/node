package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	_ "net/http/pprof"

	"github.com/ParadropLabs/node"
)

// var client *rabric.Client

var config node.NodeConfig

func main() {
	node.Log()

	// Benching code
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

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
