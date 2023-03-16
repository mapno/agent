package sampling

import (
	"context"
	"fmt"

	"github.com/grafana/agent/component/otelcol/processor/trail_sampling/internal/processor/traceql"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func NewTraceQLSampler(name, query string) *TraceQLSampler {
	return &TraceQLSampler{name: name, query: query}
}

var _ PolicyEvaluator = (*TraceQLSampler)(nil)

type TraceQLSampler struct {
	name, query string
	rateSampler PolicyEvaluator
}

func (s *TraceQLSampler) Name() string { return s.name }

func (s *TraceQLSampler) Evaluate(traceID pcommon.TraceID, td *TraceData) (Decision, error) {

	fmt.Printf("Evaluating query %s against traceID %v\n", s.query, traceID)
	matched, err := traceql.Matches(context.Background(), td.ReceivedBatches, s.query)
	if err != nil {
		return NotSampled, err
	}
	if matched {
		fmt.Printf("...Matched. Now checking sampler\n")
		if s.rateSampler == nil {
			return Sampled, nil
		}
		decision, err := s.rateSampler.Evaluate(traceID, td)
		fmt.Printf("...Sampler decision: %s\n", decision)
		return decision, err
	}

	// This means policy doesn't apply to this trace
	return Unspecified, nil
}

func (s *TraceQLSampler) WithProbabilisticSampler(r float64) {
	s.rateSampler = NewProbabilisticSampler(r)
}

func (s *TraceQLSampler) WithRateLimitingSampler(r float64) {
	s.rateSampler = NewRateLimitingSampler(r)
}
