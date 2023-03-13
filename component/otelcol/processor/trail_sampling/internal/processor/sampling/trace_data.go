package sampling

import (
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

// TraceData stores the sampling related trace data.
type TraceData struct {
	sync.Mutex
	// // Decisions gives the current status of the sampling decision for each policy.
	// Decisions []Decision // TODO: Is this needed?
	// Arrival time the first span for the trace was received.
	ArrivalTime time.Time
	// Decisiontime time when sampling decision was taken.
	DecisionTime time.Time
	// SpanCount track the number of spans on the trace.
	SpanCount *atomic.Int64
	// ReceivedBatches stores all the batches received for the trace.
	ReceivedBatches ptrace.Traces
	// FinalDecision.
	FinalDecision Decision
}
