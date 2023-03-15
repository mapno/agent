package sampling

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"golang.org/x/time/rate"
)

func NewRateLimitingSampler(r float64) *RateLimitingSampler {
	return &RateLimitingSampler{
		limiter: rate.NewLimiter(rate.Limit(r), 1),
	}
}

var _ PolicyEvaluator = (*RateLimitingSampler)(nil)

type RateLimitingSampler struct {
	limiter *rate.Limiter
}

func (s *RateLimitingSampler) Name() string { return "rate_limiting" }

func (s *RateLimitingSampler) Evaluate(pcommon.TraceID, *TraceData) (Decision, error) {
	if s.limiter.Allow() {
		return Sampled, nil
	}
	return NotSampled, nil
}
