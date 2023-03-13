package processor

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	otelcomponent "go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/grafana/agent/component/otelcol/processor/trail_sampling/internal/processor/idbatcher"
)

// policy combines a sampling policy evaluator with the destinations to be used for that policy.
type policy struct {
	// name used to identify this policy instance.
	name string

	// TODO: evaluator should be a
	// // evaluator that decides if a trace is sampled or not by this policy instance.
	// evaluator sampling.PolicyEvaluator

	// ctx used to carry metric tags of each policy.
	ctx context.Context
}

// trailSamplingProcessor handles the incoming trace data and uses the given sampling
// policy to sample traces.
type trailSamplingProcessor struct {
	ctx             context.Context
	nextConsumer    consumer.Traces
	maxNumTraces    uint64
	policies        []*policy
	logger          *zap.Logger
	idToTrace       sync.Map
	policyTicker    func()
	tickerFrequency time.Duration
	decisionBatcher idbatcher.Batcher
	deleteChan      chan pcommon.TraceID
	numTracesOnMap  *atomic.Uint64
}

const (
	sourceFormat = "trail_sampling"
)

// newTracesProcessor returns a trailSamplingProcessor.TracesProcessor that will perform tail sampling according to the given
// configuration.
func newTracesProcessor(nextConsumer consumer.Traces, cfg Config) (otelcomponent.TracesProcessor, error) {
	if nextConsumer == nil {
		return nil, otelcomponent.ErrNilNextConsumer
	}

	numDecisionBatches := uint64(10) // 10 seconds
	inBatcher, err := idbatcher.New(numDecisionBatches, 50, uint64(2*runtime.NumCPU()))
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	var policies []*policy
	// TODO: Define traceql policies

	tsp := &trailSamplingProcessor{
		ctx:          ctx,
		nextConsumer: nextConsumer,
		maxNumTraces: 100, // TODO: configure
		// logger:          logger, // TODO: configure
		decisionBatcher: inBatcher,
		policies:        policies,
		tickerFrequency: time.Second,
		numTracesOnMap:  &atomic.Uint64{},
	}

	tsp.policyTicker = func() {
		t := time.NewTicker(time.Second * 10)
		select {
		case <-t.C:
			// TODO: Do sampling policy evaluation
		case <-tsp.ctx.Done():
			t.Stop()
		}
	}
	tsp.deleteChan = make(chan pcommon.TraceID, 100)

	return tsp, nil
}

type policyMetrics struct {
	idNotFoundOnMapCount, evaluateErrorCount, decisionSampled, decisionNotSampled int64
}

// ConsumeTraces is required by the trailSamplingProcessor.Traces interface.
func (tsp *trailSamplingProcessor) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	resourceSpans := td.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		tsp.processTraces(resourceSpans.At(i))
	}
	return nil
}

func (tsp *trailSamplingProcessor) groupSpansByTraceKey(resourceSpans ptrace.ResourceSpans) map[pcommon.TraceID][]*ptrace.Span {
	idToSpans := make(map[pcommon.TraceID][]*ptrace.Span)
	ilss := resourceSpans.ScopeSpans()
	for j := 0; j < ilss.Len(); j++ {
		spans := ilss.At(j).Spans()
		spansLen := spans.Len()
		for k := 0; k < spansLen; k++ {
			span := spans.At(k)
			key := span.TraceID()
			idToSpans[key] = append(idToSpans[key], &span)
		}
	}
	return idToSpans
}

func (tsp *trailSamplingProcessor) processTraces(resourceSpans ptrace.ResourceSpans) {
	// Group spans per their traceId to minimize contention on idToTrace
	idToSpans := tsp.groupSpansByTraceKey(resourceSpans)
	var newTraceIDs int64
	for id, spans := range idToSpans {
		lenSpans := int64(len(spans))
		// lenPolicies := len(tsp.policies)
		// initialDecisions := make([]sampling.Decision, lenPolicies)
		// for i := 0; i < lenPolicies; i++ {
		// 	initialDecisions[i] = sampling.Pending
		// }
		_, loaded := tsp.idToTrace.Load(id)
		if !loaded {
			spanCount := &atomic.Int64{}
			spanCount.Store(lenSpans)
			// d, loaded = tsp.idToTrace.LoadOrStore(id, &sampling.TraceData{
			// 	Decisions:       initialDecisions,
			// 	ArrivalTime:     time.Now(),
			// 	SpanCount:       spanCount,
			// 	ReceivedBatches: ptrace.NewTraces(),
			// })
		}
		// actualData := d.(*sampling.TraceData)
		if loaded {
			// actualData.SpanCount.Add(lenSpans)
		} else {
			newTraceIDs++
			tsp.decisionBatcher.AddToCurrentBatch(id)
			tsp.numTracesOnMap.Add(1)
			postDeletion := false
			// currTime := time.Now()
			for !postDeletion {
				select {
				case tsp.deleteChan <- id:
					postDeletion = true
				default:
					_ = <-tsp.deleteChan
					// tsp.dropTrace(traceKeyToDrop, currTime)
				}
			}
		}

		// The only thing we really care about here is the final decision.
		// actualData.Lock()
		// finalDecision := actualData.FinalDecision

		// if finalDecision == sampling.Unspecified {
		// 	// If the final decision hasn't been made, add the new spans under the lock.
		// 	appendToTraces(actualData.ReceivedBatches, resourceSpans, spans)
		// 	actualData.Unlock()
		// } else {
		// 	actualData.Unlock()
		//
		// 	switch finalDecision {
		// 	case sampling.Sampled:
		// 		// Forward the spans to the policy destinations
		// 		traceTd := ptrace.NewTraces()
		// 		appendToTraces(traceTd, resourceSpans, spans)
		// 		if err := tsp.nextConsumer.ConsumeTraces(tsp.ctx, traceTd); err != nil {
		// 			tsp.logger.Warn(
		// 				"Error sending late arrived spans to destination",
		// 				zap.Error(err))
		// 		}
		// 	case sampling.NotSampled:
		// 		stats.Record(tsp.ctx, statLateSpanArrivalAfterDecision.M(int64(time.Since(actualData.DecisionTime)/time.Second)))
		// 	default:
		// 		tsp.logger.Warn("Encountered unexpected sampling decision",
		// 			zap.Int("decision", int(finalDecision)))
		// 	}
		// }
	}

	// stats.Record(tsp.ctx, statNewTraceIDReceivedCount.M(newTraceIDs))
}

func (tsp *trailSamplingProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// Start is invoked during service startup.
func (tsp *trailSamplingProcessor) Start(context.Context, otelcomponent.Host) error {
	// TODO: Start the policy ticker
	return nil
}

// Shutdown is invoked during service shutdown.
func (tsp *trailSamplingProcessor) Shutdown(context.Context) error {
	tsp.decisionBatcher.Stop()
	// TODO: Stop the policy ticker
	return nil
}

func appendToTraces(dest ptrace.Traces, rss ptrace.ResourceSpans, spans []*ptrace.Span) {
	rs := dest.ResourceSpans().AppendEmpty()
	rss.Resource().CopyTo(rs.Resource())
	ils := rs.ScopeSpans().AppendEmpty()
	for _, span := range spans {
		sp := ils.Spans().AppendEmpty()
		span.CopyTo(sp)
	}
}
