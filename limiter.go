package node

import (
	"time"
)

type Limiter interface {
	Acquire(int)
	SetLimit(int)
}

type BasicLimiter struct {
	limit int

	available   int
	windowStart int64
}

func NewBasicLimiter(limit int) *BasicLimiter {
	limiter := &BasicLimiter{
		limit:       limit,
		available:   limit,
		windowStart: time.Now().Unix(),
	}
	return limiter
}

func (limiter *BasicLimiter) Acquire(amount int) {
	// Deduct the requested amount, then delay the request until available is
	// positive again.
	limiter.available -= amount
	if limiter.available >= 0 {
		return
	}

	// Add the time since the last request.
	now := time.Now().Unix()
	elapsed := int(now - limiter.windowStart)
	if elapsed >= 1 {
		limiter.available += elapsed * limiter.limit
		limiter.windowStart = now
	}

	// If it's still negative, we have to introduce delay.
	for limiter.available < 0 {
		time.Sleep(time.Second)
		limiter.available += limiter.limit

		// If we do not move the window, they will get to double-dip next time
		// when we compute (elapsed = now - windowStart).
		limiter.windowStart++
	}

	// Do not allow for accumulation of a surplus.
	if limiter.available > limiter.limit {
		limiter.available = limiter.limit
	}
}

func (limiter *BasicLimiter) SetLimit(limit int) {
	limiter.limit = limit
}
