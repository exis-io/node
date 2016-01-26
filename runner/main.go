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

func main() {
	node.Log()

	checkEnv()

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
