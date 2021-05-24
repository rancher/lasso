package workqueue

import (
	"time"

	"golang.org/x/time/rate"
)

func NewBucketRateLimiterWithMaxTimeout(qps, burst int, max time.Duration) *BucketRateLimiterWithMaxTimeout {
	return &BucketRateLimiterWithMaxTimeout{
		Limiter:    rate.NewLimiter(rate.Limit(qps), burst),
		MaxTimeout: max,
	}
}

// BucketRateLimiterWithMaxTimeout adapts a standard bucket to the workqueue ratelimiter API
// with a max timeout since a bucket is limitless by default
type BucketRateLimiterWithMaxTimeout struct {
	*rate.Limiter
	MaxTimeout time.Duration
}

func (r *BucketRateLimiterWithMaxTimeout) When(item interface{}) time.Duration {
	timeout := r.Limiter.Reserve().Delay()
	if timeout > r.MaxTimeout {
		return r.MaxTimeout
	}
	return timeout
}

func (r *BucketRateLimiterWithMaxTimeout) NumRequeues(item interface{}) int {
	return 0
}

func (r *BucketRateLimiterWithMaxTimeout) Forget(item interface{}) {
}
