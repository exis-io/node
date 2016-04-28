package node

import (
	"encoding/json"
	"io/ioutil"
)

type ServerConfig struct {
	Certificate string
	Key         string
	Port        int
}

type NodeConfig struct {
	Agent              string
	RequestLimits      [][]interface{}
	AuthName           string
	Bouncer            string
	MessageLogFile     string
	MessageLogMaxLines int
	HoldCalls          int
	Servers            []ServerConfig
	RedisServer        string
	RedisPassword      string

	// Byte rate limit will be this multiple of message rate limit.
	ByteLimitMultiple int

	// Config file location, saved in case we need to reload.
	path string
}

func LoadConfig(path string) (*NodeConfig, error) {
	var config NodeConfig

	config.path = path

	// Default values
	config.Agent = "xs.node"
	config.AuthName = "Auth"
	config.Bouncer = "xs.demo.Bouncer"
	config.MessageLogFile = "messages.log"
	config.MessageLogMaxLines = 12500
	config.HoldCalls = 300
	config.Servers = []ServerConfig{
		ServerConfig{
			Certificate: "",
			Key:         "",
			Port:        8000,
		},
	}
	config.RedisServer = ""
	config.RedisPassword = ""
	config.ByteLimitMultiple = 1000

	out.Debug("Loading configuration file: %s", path)
	err := config.Reload()

	return &config, err
}

// Get request limit from configuration file.
// This is the number of requests (per second) a domain is allowed to make.
//
// Entries are checked in order, and the first entry that contains (equal to or
// larger than) the given domain is taken.
//
// Be sure to include a default in the configuration file, e.g. ["", 100] as
// the last entry.
func (config *NodeConfig) GetRequestLimit(domain string) int {
	for _, entry := range config.RequestLimits {
		// entry should be ["domain", limit]
		entryDomain, _ := entry[0].(string)
		entryLimit := int(entry[1].(float64))

		if entryDomain == "" || subdomain(entryDomain, domain) {
			return entryLimit
		}
	}

	out.Critical("No default request limit defined: returning 1")
	return 1
}

func (config *NodeConfig) Reload() error {
	file, err := ioutil.ReadFile(config.path)
	if err != nil {
		out.Critical("Loading configuration file failed: %s", err)
		return err
	}

	err = json.Unmarshal(file, config)
	if err != nil {
		out.Critical("Parsing configuration file failed: %s", err)
		return err
	}

	return nil
}
