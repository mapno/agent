package sampling

import (
	"context"
	"fmt"

	"github.com/grafana/agent/component/otelcol/processor/trail_sampling/internal/processor/traceql"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func NewTraceQLSampler(query string) *TraceQLSampler {
	return &TraceQLSampler{query: query}
}

var _ PolicyEvaluator = (*TraceQLSampler)(nil)

type TraceQLSampler struct {
	query       string
	rateSampler PolicyEvaluator
}

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
		fmt.Printf("...Sampler decision: %v\n", decision)
		return decision, err
	}

	// This means policy doesn't apply to this trace
	return Unspecified, nil
}

func (s *TraceQLSampler) WithProbabilitySampler(r float64) {
	s.rateSampler = NewProbabilisticSampler(r)
}
