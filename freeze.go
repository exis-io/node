// Methods for handling frozen sessions
//
// Here, frozen means that the session was intentionally closed but enough
// state is available to resume the session on demand.

package node

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
)

type frozenRegistration struct {
	thawEndpoint string
	thawID       string
	sessionID    int64
}

func NewSessionID(pool *redis.Pool, domain string) (ID, error) {
	conn := pool.Get()
	defer conn.Close()

	for {
		sessionID, err := redis.Int64(conn.Do("INCR", "session:id"))
		if err != nil {
			fmt.Println(err)
			return ID(0), err
		}

		sessionKey := fmt.Sprintf("session:%x", sessionID)

		reply, err := redis.Int(conn.Do("HSETNX", sessionKey, "domain", domain))
		if err != nil {
			fmt.Println(err)
			return ID(0), err
		} else if reply == 1 {
			// Done: acquired a unique session ID.
			return ID(sessionID), nil
		}
	}
}

func ReclaimSessionID(pool *redis.Pool, sessionID ID, authid string, domain string) error {
	conn := pool.Get()
	defer conn.Close()

	sessionKey := fmt.Sprintf("session:%x", sessionID)

	// First, try to claim the session ID.  This tells us if it exists or
	// safely reserves it if it does not.
	reply, err := redis.Int(conn.Do("HSETNX", sessionKey, "domain", domain))
	if err != nil {
		fmt.Println(err)
		return err
	} else if reply == 1 {
		// It did not exist before, but now he owns it.
		return nil
	}

	prevDomain, err := redis.String(conn.Do("HGET", sessionKey, "domain"))
	if err != nil {
		fmt.Println(err)
		return err
	}

	// Ensure that the new agent owns the claimed session ID.
	if subdomain(authid, prevDomain) {
		return nil
	} else {
		return fmt.Errorf("Permission denied: %s cannot claim %s", authid, sessionKey)
	}
}

func GetFrozenRegistration(pool *redis.Pool, endpoint string) (*frozenRegistration, error) {
	conn := pool.Get()
	defer conn.Close()

	regKey := fmt.Sprintf("registered:%s", endpoint)
	reply, err := redis.Values(conn.Do("ZREVRANGE", regKey, 0, -1))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	var sessionIDs []int64
	if err := redis.ScanSlice(reply, &sessionIDs); err != nil {
		fmt.Println(err)
		return nil, err
	}

	if len(sessionIDs) == 0 {
		return nil, fmt.Errorf("No registration found for %s", endpoint)
	}

	for _, sessionid := range sessionIDs {
		freg := &frozenRegistration{
			sessionID: sessionid,
		}

		sessionKey := fmt.Sprintf("session:%x", sessionid)

		freg.thawEndpoint, err = redis.String(conn.Do("HGET", sessionKey, "thawEndpoint"))
		if err != nil {
			fmt.Println(err)
			continue
		}

		freg.thawID, err = redis.String(conn.Do("HGET", sessionKey, "thawID"))
		if err != nil {
			fmt.Println(err)
			continue
		}

		return freg, nil
	}

	return nil, fmt.Errorf("Not implemented")
}

func StoreFrozenRegistration(pool *redis.Pool, reg *frozenRegistration) error {
	return nil
}
