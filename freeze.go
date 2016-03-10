// Methods for handling frozen sessions
//
// Here, frozen means that the session was intentionally closed but enough
// state is available to resume the session on demand.

package node

import (
	"fmt"
	"time"
	"github.com/garyburd/redigo/redis"
)

const (
	ExpireTime int = 300
)

type redisSetIterator struct {
    conn   redis.Conn
    key    string
    cursor int
    values []interface{}
}

func (iter *redisSetIterator) Int64() int64 {
	x, _ := redis.Int64(iter.values[0], nil)
	return x
}

func (iter *redisSetIterator) String() string {
	x, _ := redis.String(iter.values[0], nil)
	return x
}

func (iter *redisSetIterator) Next() bool {
    if len(iter.values) > 0 {
        iter.values = iter.values[1:]
    }

    for len(iter.values) == 0 && iter.cursor != 0 {
        // Special initialization case: -1 means this is the first query.
        if iter.cursor == -1 {
            iter.cursor = 0
        }

        arr, err := redis.Values(iter.conn.Do("SSCAN", iter.key, iter.cursor))
        if err != nil {
            return false
        }

        iter.cursor, _ = redis.Int(arr[0], nil)
		iter.values, _ = redis.Values(arr[1], nil)
    }

    return (len(iter.values) > 0)
}

func (iter *redisSetIterator) Close() {
    if iter.conn != nil {
        iter.conn.Close()
        iter.conn = nil
    }
}

func NewSessionID(pool *redis.Pool, domain string) (ID, error) {
	conn := pool.Get()
	defer conn.Close()

	for {
		sessionID, err := redis.Int64(conn.Do("INCR", "session:id"))
		if err != nil {
			out.Debug("Redis error on key session:id: %v", err)
			return ID(0), err
		}

		sessionKey := fmt.Sprintf("session:%x", sessionID)

		reply, err := redis.Int(conn.Do("HSETNX", sessionKey, "domain", domain))
		if err != nil {
			out.Debug("Redis error on key %s: %v", sessionKey, err)
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
		out.Debug("Redis error on key %s: %v", sessionKey, err)
		return err
	} else if reply == 1 {
		// It did not exist before, but now he owns it.
		return nil
	}

	prevDomain, err := redis.String(conn.Do("HGET", sessionKey, "domain"))
	if err != nil {
		out.Debug("Redis error on key %s: %v", sessionKey, err)
		return err
	}

	// Ensure that the new agent owns the claimed session ID.
	if subdomain(authid, prevDomain) {
		return nil
	} else {
		return fmt.Errorf("Permission denied: %s cannot claim %s", authid, sessionKey)
	}
}

// ResumeSessionPermitted
// Checks if the session ID exists and is owned by the given authid.
func ResumeSessionPermitted(pool *redis.Pool, session ID, authid string) bool {
	conn := pool.Get()
	defer conn.Close()

	sessionKey := fmt.Sprintf("session:%x", session)

	prevDomain, err := redis.String(conn.Do("HGET", sessionKey, "domain"))
	if err != nil || prevDomain == "" {
		return false
	}

	return subdomain(authid, prevDomain)
}

func StoreSessionDetails(pool *redis.Pool, session *Session, details map[string]interface{}) {
	conn := pool.Get()
	defer conn.Close()

	sessionKey := fmt.Sprintf("session:%x", session.Id)

	if !session.canFreeze {
		// Track sessions that cannot be frozen (ordinary ones) so that the
		// node can clear them out when it restarts.
		_, err := conn.Do("SADD", "transient_sessionids", int64(session.Id))
		if err != nil {
			out.Debug("Redis error on key transient_sessionids: %v", err)
		}
	}

	guardian, ok := details["guardianDomain"].(string)
	if ok && guardian != "" {
		endpoint := guardian + "/thaw"
		_, err := conn.Do("HSET", sessionKey, "thawEndpoint", endpoint)
		if err != nil {
			out.Debug("Redis error on key %s: %v", sessionKey, err)
		}
	}

	id, ok := details["guardianID"].(string)
	if ok && id != "" {
		_, err := conn.Do("HSET", sessionKey, "thawID", id)
		if err != nil {
			out.Debug("Redis error on key %s: %v", sessionKey, err)
		}
	}
}

func removeSession(conn redis.Conn, session ID) {
	RemoveRegistrations(conn, session)
	RemoveSubscriptions(conn, session)

	sessionKey := fmt.Sprintf("session:%x", session)
	_, err := conn.Do("DEL", sessionKey)
	if err != nil {
		out.Debug("Redis error on key %s: %v", sessionKey, err)
	}
}

func RedisRemoveSession(pool *redis.Pool, session ID) {
	conn := pool.Get()
	defer conn.Close()

	removeSession(conn, session)
}

func ThawSession(pool *redis.Pool, agent *Client, session ID) error {
	// TODO This function should only be fired once even if multiple
	// messages arrive for the frozen session.

	go func() {
		time.Sleep(time.Duration(ExpireTime) * time.Second)
		RedisRemoveSession(pool, session)
	}()

	conn := pool.Get()
	defer conn.Close()

	sessionKey := fmt.Sprintf("session:%x", session)

	thawEndpoint, err := redis.String(conn.Do("HGET", sessionKey, "thawEndpoint"))
	if err != nil {
		out.Debug("Redis error on key %s: %v", sessionKey, err)
		return err
	}

	thawID, err := redis.String(conn.Do("HGET", sessionKey, "thawID"))
	if err != nil {
		out.Debug("Redis error on key %s: %v", sessionKey, err)
		return err
	}

	args := []interface{}{thawID, int64(session)}
	result, err := agent.Call(thawEndpoint, args, nil)
	if err != nil {
		out.Debug("Error from thaw method %s: %s", thawEndpoint, err.Error())
		return err
	}

	code, ok := result.Arguments[0].(int)
	if ok && code == 0 {
		return nil
	} else {
		return fmt.Errorf("Non-zero return from thaw method %s: %v", thawEndpoint, result)
	}
}

func RemoveRegistrations(conn redis.Conn, session ID) {
    iter := redisSetIterator{
        conn: conn,
        key: fmt.Sprintf("registered:%x", session),
        cursor: -1,
    }

    for iter.Next() {
        registrationKey := iter.String()

        endpoint, err := redis.String(conn.Do("HGET", registrationKey, "endpoint"))
        if err != nil {
            out.Debug("Redis error from key %s: %v", registrationKey, err)
        }

        if endpoint != "" {
            proceduresKey := fmt.Sprintf("procedures:%s", endpoint)
            _, err := conn.Do("ZREM", proceduresKey, registrationKey)
            if err != nil {
                out.Debug("Redis error from key %s: %v", proceduresKey, err)
            }
        }

        _, err = conn.Do("DEL", registrationKey)
        if err != nil {
            out.Debug("Redis error from key %s: %v", registrationKey, err)
        }
    }

    _, err := conn.Do("DEL", iter.key)
    if err != nil {
        out.Debug("Redis error from key %s: %v", iter.key, err)
    }
}

func RemoveSubscriptions(conn redis.Conn, session ID) {
    // Leverage the iterator code to walk the subscriptions for a session
    // rather than endpoint.
    // Do not close the iterator because we will close the connection later.
    iter := &subscriptionIterator{
        conn: conn,
        key: fmt.Sprintf("subscribed:%x", session),
        cursor: -1,
    }

    for iter.Next() {
        sub := iter.Value()

        subscriptionKey := fmt.Sprintf("subscription:%x:%x", session, sub.ID)

        subscribersKey := fmt.Sprintf("subscribers:%s", sub.Endpoint)
        _, err := conn.Do("SREM", subscribersKey, subscriptionKey)
        if err != nil {
            out.Debug("Redis error from key %s: %v", subscribersKey, err)
        }

        _, err = conn.Do("DEL", subscriptionKey)
        if err != nil {
            out.Debug("Redis error from key %s: %v", subscriptionKey, err)
        }
    }

    _, err := conn.Do("DEL", iter.key)
    if err != nil {
        out.Debug("Redis error from key %s: %v", iter.key, err)
    }
}

func ClearTransientSessions(pool *redis.Pool) {
	conn := pool.Get()
	defer conn.Close()

    iter := redisSetIterator{
        conn: conn,
        key: "transient_sessionids",
        cursor: -1,
    }

    for iter.Next() {
		id := ID(iter.Int64())
		removeSession(conn, id)
	}

	_, err := conn.Do("DEL", "transient_sessionids")
	if err != nil {
        out.Debug("Redis error from key transient_sessionids: %v", err)
	}
}
