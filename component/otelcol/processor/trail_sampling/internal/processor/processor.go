package processor

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/grafana/agent/component/otelcol/processor/trail_sampling/internal/processor/sampling"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	otelcomponent "go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/grafana/agent/component/otelcol/processor/trail_sampling/internal/processor/idbatcher"
)

var (
	metricPolicyTraces = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "trail_sampling_policy_spans_total",
	}, []string{"policy", "decision"})
	metricPolicySpans = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "trail_sampling_policy_traces_total",
	}, []string{"policy", "decision"})
	/*metricTraces = promauto.NewCounter(prometheus.CounterOpts{
		Name: "trail_sampling_traces_total",
	})
	metricSpans = promauto.NewCounter(prometheus.CounterOpts{
		Name: "trail_sampling_spans_total",
	})*/
)

// trailSamplingProcessor handles the incoming trace data and uses the given sampling
// policy to sample traces.
type trailSamplingProcessor struct {
	ctx          context.Context
	logger       *zap.Logger // TODO: Use better logger
	nextConsumer consumer.Traces

	policies []sampling.PolicyEvaluator

	decisionBatcher idbatcher.Batcher
	idToTrace       sync.Map
	numTracesOnMap  *atomic.Uint64

	policyTicker    sampling.PolicyTicker
	tickerFrequency time.Duration
	deleteChan      chan pcommon.TraceID
}

const (
	sourceFormat = "trail_sampling"
)

// newTracesProcessor returns a trailSamplingProcessor.TracesProcessor that will perform trail sampling according to the given configuration.
func newTracesProcessor(ctx context.Context, nextConsumer consumer.Traces, cfg Config) (otelcomponent.TracesProcessor, error) {
	if nextConsumer == nil {
		return nil, otelcomponent.ErrNilNextConsumer
	}

	numDecisionBatches := uint64(cfg.DecisionWait.Seconds())
	inBatcher, err := idbatcher.New(numDecisionBatches, cfg.ExpectedNewTracesPerSec, uint64(2*runtime.NumCPU()))
	if err != nil {
		return nil, err
	}

	var policies []sampling.PolicyEvaluator
	for _, policyCfg := range cfg.PolicyCfgs {
		policy := sampling.NewTraceQLSampler(policyCfg.Name, policyCfg.Query)
		policy.WithProbabilitySampler(policyCfg.Probabilistic)

		policies = append(policies, policy)
	}

	tsp := &trailSamplingProcessor{
		ctx:             ctx,
		nextConsumer:    nextConsumer,
		logger:          zap.NewExample(), // TODO: configure
		decisionBatcher: inBatcher,
		policies:        policies,
		tickerFrequency: time.Second,
		numTracesOnMap:  &atomic.Uint64{},
	}

	tsp.policyTicker = &sampling.Ticker{OnTickFunc: tsp.samplingPolicyOnTick}
	tsp.deleteChan = make(chan pcommon.TraceID, cfg.NumTraces)

	return tsp, nil
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
			// if key.IsEmpty() {
			// 	continue
			// }
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

		d, loaded := tsp.idToTrace.Load(id)
		if !loaded {
			spanCount := &atomic.Int64{}
			spanCount.Store(lenSpans)
			d, loaded = tsp.idToTrace.LoadOrStore(id, &sampling.TraceData{
				ArrivalTime:     time.Now(),
				SpanCount:       spanCount,
				ReceivedBatches: ptrace.NewTraces(),
				FinalDecision:   sampling.Unspecified,
			})
		}
		actualData := d.(*sampling.TraceData)
		if loaded {
			actualData.SpanCount.Add(lenSpans)
		} else {
			newTraceIDs++
			tsp.decisionBatcher.AddToCurrentBatch(id)
			tsp.numTracesOnMap.Add(1)
			postDeletion := false
			currTime := time.Now()
			for !postDeletion {
				select {
				case tsp.deleteChan <- id:
					postDeletion = true
				default:
					traceKeyToDrop := <-tsp.deleteChan
					tsp.dropTrace(traceKeyToDrop, currTime)
				}
			}
		}

		// The only thing we really care about here is the final decision.
		actualData.Lock()
		finalDecision := actualData.FinalDecision

		switch finalDecision {
		case sampling.Unspecified:
			// If the final decision hasn't been made, add the new spans under the lock.
			appendToTraces(actualData.ReceivedBatches, resourceSpans, spans)
			actualData.Unlock()

		case sampling.Sampled:
			actualData.Unlock()
			// Forward the spans to the policy destinations
			traceTd := ptrace.NewTraces()
			appendToTraces(traceTd, resourceSpans, spans)
			if err := tsp.nextConsumer.ConsumeTraces(tsp.ctx, traceTd); err != nil {
				tsp.logger.Warn(
					"Error sending late arrived spans to destination",
					zap.Error(err))
			}

		case sampling.NotSampled:
			actualData.Unlock()
			// stats.Record(tsp.ctx, statLateSpanArrivalAfterDecision.M(int64(time.Since(actualData.DecisionTime)/time.Second)))

		default:
			actualData.Unlock()
			tsp.logger.Warn("Encountered unexpected sampling decision",
				zap.Int("decision", int(finalDecision)))
		}
	}

	// stats.Record(tsp.ctx, statNewTraceIDReceivedCount.M(newTraceIDs))
}

func (tsp *trailSamplingProcessor) samplingPolicyOnTick() {
	// metrics := policyMetrics{}

	// startTime := time.Now()
	batch, _ := tsp.decisionBatcher.CloseCurrentAndTakeFirstBatch()
	batchLen := len(batch)
	tsp.logger.Debug("Sampling Policy Evaluation ticked")
	for _, id := range batch {
		d, ok := tsp.idToTrace.Load(id)
		if !ok {
			// metrics.idNotFoundOnMapCount++
			continue
		}
		trace := d.(*sampling.TraceData)
		trace.DecisionTime = time.Now()

	policies:
		for _, policy := range tsp.policies {
			tsp.logger.Debug("Evaluating policy", zap.String("policy", policy.Name()))
			decision, err := policy.Evaluate(id, trace)
			if err != nil {
				// metrics.evaluateErrorCount++
				tsp.logger.Warn("Error evaluating sampling policy", zap.Error(err))
				continue
			}

			recordPolicyMetrics(policy.Name(), decision, trace)

			switch decision {
			case sampling.NotSampled, sampling.Sampled:
				trace.FinalDecision = decision
				// Exit early
				break policies
			case sampling.Unspecified:
			}
		}

		// If no policy matched, then the trace is not sampled
		if trace.FinalDecision == sampling.Unspecified {
			trace.FinalDecision = sampling.NotSampled

			recordPolicyMetrics("__default__", sampling.NotSampled, trace)
		}

		//recordTotalMetrics(trace)

		// Sampled or not, remove the batches
		trace.Lock()
		allSpans := ptrace.NewTraces()
		trace.ReceivedBatches.MoveTo(allSpans)
		trace.Unlock()

		if trace.FinalDecision == sampling.Sampled {
			tsp.logger.Debug("Sampled spans", zap.Int("count", allSpans.SpanCount()))
			_ = tsp.nextConsumer.ConsumeTraces(context.TODO(), allSpans)
		}
	}

	// stats.Record(tsp.ctx,
	// 	statOverallDecisionLatencyUs.M(int64(time.Since(startTime)/time.Microsecond)),
	// 	statDroppedTooEarlyCount.M(metrics.idNotFoundOnMapCount),
	// 	statPolicyEvaluationErrorCount.M(metrics.evaluateErrorCount),
	// 	statTracesOnMemoryGauge.M(int64(tsp.numTracesOnMap.Load())))

	tsp.logger.Debug("Sampling policy evaluation completed",
		zap.Int("batch.len", batchLen),
		// zap.Int64("sampled", metrics.decisionSampled),
		// zap.Int64("notSampled", metrics.decisionNotSampled),
		// zap.Int64("droppedPriorToEvaluation", metrics.idNotFoundOnMapCount),
		// zap.Int64("policyEvaluationErrors", metrics.evaluateErrorCount),
	)
}

func (tsp *trailSamplingProcessor) dropTrace(traceID pcommon.TraceID, deletionTime time.Time) {
	var trace *sampling.TraceData
	if d, ok := tsp.idToTrace.Load(traceID); ok {
		trace = d.(*sampling.TraceData)
		tsp.idToTrace.Delete(traceID)
		// Subtract one from numTracesOnMap per https://godoc.org/sync/atomic#AddUint64
		tsp.numTracesOnMap.Add(^uint64(0))
	}
	if trace == nil {
		tsp.logger.Error("Attempt to delete traceID not on table")
		return
	}

	// stats.Record(tsp.ctx, statTraceRemovalAgeSec.M(int64(deletionTime.Sub(trace.ArrivalTime)/time.Second)))
}

func (tsp *trailSamplingProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// Start is invoked during service startup.
func (tsp *trailSamplingProcessor) Start(context.Context, otelcomponent.Host) error {
	tsp.policyTicker.Start(time.Second)
	return nil
}

// Shutdown is invoked during service shutdown.
func (tsp *trailSamplingProcessor) Shutdown(context.Context) error {
	tsp.decisionBatcher.Stop()
	tsp.policyTicker.Stop()
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

func recordPolicyMetrics(policy string, decision sampling.Decision, t *sampling.TraceData) {
	var decisionLabel string
	switch decision {
	case sampling.Sampled:
		decisionLabel = "sampled"
	case sampling.NotSampled:
		decisionLabel = "dropped"
	default:
		return
	}

	metricPolicyTraces.WithLabelValues(policy, "matched").Inc()
	metricPolicyTraces.WithLabelValues(policy, decisionLabel).Inc()

	spanCount := float64(t.ReceivedBatches.SpanCount())
	metricPolicySpans.WithLabelValues(policy, "matched").Add(spanCount)
	metricPolicySpans.WithLabelValues(policy, decisionLabel).Add(spanCount)
}

/*func recordTotalMetrics(t *sampling.TraceData) {
	metricTraces.Inc()
	metricSpans.Add(float64(t.ReceivedBatches.SpanCount()))
}*/
