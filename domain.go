package rabric

import (
	"regexp"
	"strings"
)

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

// Extract action from endpoint, returning an error
func extractActions(s string) (string, error) {
	i := strings.Index(s, "/")

	// No slash found, error
	if i == -1 {
		return "", InvalidURIError(s)
	}

	// not covered: closing slash
	// Is this a bug? Intentional? Should be considered as part of the action?
	// pd.damouse/

	// i += 1
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
