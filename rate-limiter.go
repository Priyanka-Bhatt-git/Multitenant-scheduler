package scheduler

import (
	"errors"
	"time"
)

var ErrRateLimited = errors.New("tenant rate limit exceeded")

// TenantRateLimit defines a token-bucket policy for a tenant.
type TenantRateLimit struct {
	Rate     float64 // tokens added per second
	Capacity int     // max burst size
}

// tokenBucket stores the mutable state of a tenant's bucket.
type tokenBucket struct {
	rate       float64
	capacity   int
	tokens     float64
	lastRefill time.Time
}

func newTokenBucket(rate float64, capacity int) *tokenBucket {
	now := time.Now()
	return &tokenBucket{
		rate:       rate,
		capacity:   capacity,
		tokens:     float64(capacity), // start full so initial burst is allowed
		lastRefill: now,
	}
}

// allow refills tokens based on elapsed time and consumes one token if available.
func (tb *tokenBucket) allow(now time.Time) bool {
	if tb == nil {
		return true
	}
	if tb.rate <= 0 || tb.capacity <= 0 {
		return true // treat invalid / zero config as "unlimited"
	}

	elapsed := now.Sub(tb.lastRefill).Seconds()
	if elapsed > 0 {
		tb.tokens += elapsed * tb.rate
		if tb.tokens > float64(tb.capacity) {
			tb.tokens = float64(tb.capacity)
		}
		tb.lastRefill = now
	}

	if tb.tokens < 1 {
		return false
	}

	tb.tokens--
	return true
}
