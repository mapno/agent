package sampling

import "go.opentelemetry.io/collector/pdata/pcommon"

type Decision int

const (
	// Sampled means that the trace was sampled.
	Sampled Decision = iota
	// NotSampled means that the trace was not sampled.
	NotSampled
	// Unspecified means that the decision was not set.
	Unspecified
)

func (d Decision) String() string {
	switch d {
	case Sampled:
		return "sampled"
	case NotSampled:
		return "not_sampled"
	case Unspecified:
		return "unspecified"
	default:
		return "unknown"
	}
}

type PolicyEvaluator interface {
	Name() string
	// Evaluate evaluates the policy for the given trace.
	Evaluate(traceID pcommon.TraceID, td *TraceData) (Decision, error)
}
