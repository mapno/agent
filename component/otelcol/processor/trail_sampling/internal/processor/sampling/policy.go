package sampling

import "go.opentelemetry.io/collector/pdata/pcommon"

type Decision int

const (
	// Pending is the default decision when a trace is first seen.
	Pending Decision = iota
	// Sampled means that the trace was sampled.
	Sampled
	// NotSampled means that the trace was not sampled.
	NotSampled
	// Unspecified means that the trace was not sampled.
	Unspecified
)

type Policy struct {
	Name     string
	Evaluate func(traceID pcommon.TraceID, trace *TraceData) (Decision, error)
}
