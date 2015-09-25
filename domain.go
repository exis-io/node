package node

import (
	"regexp"
	"strings"
)

const ACTION_SEPARATOR string = "/"
const DOMAIN_SEPARATOR string = "."

// Check out the golang tester for more info:
// https://regex-golang.appspot.com/assets/html/index.html
func validEndpoint(s string) bool {
	r, _ := regexp.Compile("(^([a-z]+)(.[a-z]+)*$)")
	return r.MatchString(s)
}

func validDomain(s string) bool {
	return true
}

func validAction(s string) bool {
	return true
}

// Extract action from endpoint. Endpoint without a closing slash
// indicating an action is considered an error.
func extractActions(s string) (string, error) {
	i := strings.Index(s, "/")

	// No slash found, error
	if i == -1 {
		return "", InvalidURIError(s)
	}

	i += 1
	return s[i:], nil
}

// Extract domain from endpoint, returning an error
func extractDomain(s string) (string, error) {
	i := strings.Index(s, "/")
	// out.Critical("Index of string: %s", i)

	if i == -1 {
		return "", InvalidURIError(s)
	}

	return s[:i], nil
}

// Checks if the target domain is "down" from the given domain.
// That is-- it is either a subdomain or the same domain.
// Assumes the passed domains are well constructed.
// Agent should be purely a domain string, target may be a domain or an
// endpoint.
func subdomain(agent, target string) bool {
	targetDomain, err := extractDomain(target)
	if err != nil {
		// Error means the target was already a domain (no action string).
		targetDomain = target
	}

	agentParts := strings.Split(agent, DOMAIN_SEPARATOR)
	targetParts := strings.Split(targetDomain, DOMAIN_SEPARATOR)

	// Target cannot be a subdomain of agent if it is shorter.
	if len(targetParts) < len(agentParts) {
		return false
	}

	for i := 0; i < len(agentParts) && i < len(targetParts); i++ {
		if targetParts[i] != agentParts[i] {
			return false
		}
	}

	return true
}

// breaks down an endpoint into domain and action, or returns an error
func breakdownEndpoint(s string) (string, string, error) {
	d, errDomain := extractDomain(s)

	if errDomain != nil {
		return "", "", InvalidURIError(s)
	}

	a, errAction := extractActions(s)

	if errAction != nil {
		return "", "", InvalidURIError(s)
	}

	return d, a, nil
}
