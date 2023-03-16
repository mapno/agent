package sampling

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestRateLimitingSampler(t *testing.T) {
	s := NewRateLimitingSampler(1)

	// First call should always be sampled
	decision, err := s.Evaluate(pcommon.TraceID{}, nil)
	assert.NoError(t, err)
	assert.Equal(t, Sampled, decision)

	// Second call should always be not sampled
	decision, err = s.Evaluate(pcommon.TraceID{}, nil)
	assert.NoError(t, err)
	assert.Equal(t, NotSampled, decision)

	// After 1s, another trace should be sampler
	time.Sleep(time.Second)
	decision, err = s.Evaluate(pcommon.TraceID{}, nil)
	assert.NoError(t, err)
	assert.Equal(t, Sampled, decision)
}
