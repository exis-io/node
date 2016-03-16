package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/exis-io/node"
)

// var client *rabric.Client

var config node.NodeConfig

// Default environment variable values for use with checkEnv.
var envDefaults = map[string]string {
	"EXIS_CERT": "",
	"EXIS_KEY": "",
	"EXIS_PERMISSIONS": "on",
	"EXIS_AUTHENTICATION": "on",
}

// Check what environment variables are currently set.
func checkEnv() {
	environ := os.Environ()

	envmap := map[string]string{}
	for _, ev := range environ {
		parts := strings.Split(ev, "=")
		envmap[parts[0]] = parts[1]
	}

	for k, v := range envDefaults {
		if val, ok := envmap[k]; ok {
			fmt.Printf("Environment: %s=%s (SET)\n", k, val)
		} else {
			fmt.Printf("Environment: %s=%s (DEFAULT)\n", k, v)
		}
	}
}

func runHTTPServer(handler *node.WebsocketServer, config node.ServerConfig) {
	server := &http.Server{
		Handler: handler,
		Addr:    fmt.Sprintf(":%d", config.Port),
	}

	if config.Certificate != "" && config.Key != "" {
		log.Fatal(server.ListenAndServeTLS(config.Certificate, config.Key))
	} else {
		log.Fatal(server.ListenAndServe())
	}
}

func main() {
	node.Log()

	checkEnv()

	var configPath = flag.String("configpath", "config.json", "the configuration file for the node")
	flag.Parse()
	config, _ := node.LoadConfig(*configPath)

	// Pass certificate here
	s := node.CreateNode(config)

	if len(config.Servers) == 0 {
		// Old config uses environment variables to get certificate and key
		// path and is hard-coded to port 8000.
		serverConfig := node.ServerConfig{
			Certificate: os.Getenv("EXIS_CERT"),
			Key: os.Getenv("EXIS_KEY"),
			Port: 8000,
		}

		runHTTPServer(s, serverConfig)
	} else {
		for i, serverConfig := range(config.Servers) {
			if i+1 < len(config.Servers) {
				go runHTTPServer(s, serverConfig)
			} else {
				runHTTPServer(s, serverConfig)
			}
		}
	}
}
