package sampling

import (
	"context"

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
	if s.rateSampler != nil {
		if decision, err := s.rateSampler.Evaluate(traceID, td); err != nil || decision != Sampled {
			return decision, err
		}
	}

	matched, err := traceql.Matches(context.Background(), td.ReceivedBatches, s.query)
	if err != nil {
		return NotSampled, err
	}
	if matched {
		return Sampled, nil
	}
	return Pending, nil
}

func (s *TraceQLSampler) WithProbabilitySampler(r float64) {
	s.rateSampler = NewProbabilisticSampler(r)
}
