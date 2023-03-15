package processor

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/grafana/agent/component/otelcol/processor/trail_sampling/internal/processor/sampling"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/spanmetricsprocessor/mocks"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	v1_common "go.opentelemetry.io/proto/otlp/common/v1"
	v1_resource "go.opentelemetry.io/proto/otlp/resource/v1"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestTrailSamplingProcessor_HappyPath(t *testing.T) {
	cfg := Config{
		DecisionWait:            time.Second,
		NumTraces:               100,
		ExpectedNewTracesPerSec: 2,
		PolicyCfgs: []PolicyCfg{
			{
				Name:          "drop service.name = service2",
				Query:         `{ resource.service.name = "service2" }`,
				Probabilistic: 0,
			},
			{
				Name:          "base (fallthrough)",
				Query:         "{}",
				Probabilistic: 1,
			},
		},
	}

	nextConsumer := new(consumertest.TracesSink)

	s, err := newTracesProcessor(context.TODO(), nextConsumer, cfg)
	assert.NoError(t, err)

	s.(*trailSamplingProcessor).policyTicker = &mockTicker{}

	mockHost := &mocks.Host{}
	assert.NoError(t, s.Start(context.TODO(), mockHost))

	var id [16]byte
	_, err = rand.Read(id[:])
	assert.NoError(t, err)

	td, err := fromProto(fullyPopulatedTestTrace(id))
	assert.NoError(t, err)

	assert.NoError(t, s.ConsumeTraces(context.TODO(), td))
	s.(*trailSamplingProcessor).samplingPolicyOnTick() // Drain empty batches
	s.(*trailSamplingProcessor).samplingPolicyOnTick() // Drain empty batches
	s.(*trailSamplingProcessor).samplingPolicyOnTick()

	sampledTraces := nextConsumer.AllTraces()
	assert.Len(t, sampledTraces, 1)
	sampledTraceID := sampledTraces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).TraceID()
	assert.Equal(t, pcommon.TraceID(id), sampledTraceID)
}

var _ sampling.PolicyTicker = (*mockTicker)(nil)

type mockTicker struct{}

func (m *mockTicker) Start(time.Duration) {}

func (m *mockTicker) OnTick() {}

func (m *mockTicker) Stop() {}

const (
	LabelRootSpanName    = "root.name"
	LabelRootServiceName = "root.service.name"

	LabelServiceName = "service.name"
	LabelCluster     = "cluster"
	LabelNamespace   = "namespace"
	LabelPod         = "pod"
	LabelContainer   = "container"

	LabelK8sClusterName   = "k8s.cluster.name"
	LabelK8sNamespaceName = "k8s.namespace.name"
	LabelK8sPodName       = "k8s.pod.name"
	LabelK8sContainerName = "k8s.container.name"

	LabelName           = "name"
	LabelHTTPMethod     = "http.method"
	LabelHTTPUrl        = "http.url"
	LabelHTTPStatusCode = "http.status_code"
	LabelStatusCode     = "status.code"
	LabelStatus         = "status"
	LabelDuration       = "duration"
)

func fromProto(trace *v1.TracesData) (ptrace.Traces, error) {
	buf, err := proto.Marshal(trace)
	if err != nil {
		return ptrace.Traces{}, err
	}

	u := &ptrace.ProtoUnmarshaler{}
	return u.UnmarshalTraces(buf)
}

func fullyPopulatedTestTrace(id [16]byte) *v1.TracesData {

	strPtr := func(s string) *v1_common.AnyValue {
		return &v1_common.AnyValue{Value: &v1_common.AnyValue_StringValue{StringValue: s}}
	}
	intPtr := func(i int64) *v1_common.AnyValue {
		return &v1_common.AnyValue{Value: &v1_common.AnyValue_IntValue{IntValue: i}}
	}
	fltPtr := func(f float64) *v1_common.AnyValue {
		return &v1_common.AnyValue{Value: &v1_common.AnyValue_DoubleValue{DoubleValue: f}}
	}
	boolPtr := func(b bool) *v1_common.AnyValue {
		return &v1_common.AnyValue{Value: &v1_common.AnyValue_BoolValue{BoolValue: b}}
	}

	return &v1.TracesData{

		ResourceSpans: []*v1.ResourceSpans{
			{
				Resource: &v1_resource.Resource{

					Attributes: []*v1_common.KeyValue{
						{Key: LabelServiceName, Value: strPtr("myservice")},
						{Key: LabelCluster, Value: strPtr("cluster")},
						{Key: LabelNamespace, Value: strPtr("namespace")},
						{Key: LabelPod, Value: strPtr("pod")},
						{Key: LabelContainer, Value: strPtr("container")},
						{Key: LabelK8sClusterName, Value: strPtr("k8scluster")},
						{Key: LabelK8sNamespaceName, Value: strPtr("k8snamespace")},
						{Key: LabelK8sPodName, Value: strPtr("k8spod")},
						{Key: LabelK8sContainerName, Value: strPtr("k8scontainer")},
						{Key: "foo", Value: strPtr("abc")},
						{Key: "int", Value: intPtr(123)},
					},
				},
				ScopeSpans: []*v1.ScopeSpans{
					{
						Spans: []*v1.Span{
							{
								SpanId:                 []byte("spanid00"),
								TraceId:                id[:],
								Name:                   "hello",
								StartTimeUnixNano:      uint64(100 * time.Second),
								EndTimeUnixNano:        uint64(200 * time.Second),
								ParentSpanId:           []byte{},
								TraceState:             "tracestate",
								Kind:                   v1.Span_SPAN_KIND_CLIENT,
								DroppedAttributesCount: 42,
								DroppedEventsCount:     43,
								Status: &v1.Status{
									Code:    v1.Status_STATUS_CODE_ERROR,
									Message: v1.Status_STATUS_CODE_ERROR.String(),
								},
								Attributes: []*v1_common.KeyValue{
									{Key: LabelHTTPMethod, Value: strPtr("get")},
									{Key: LabelHTTPUrl, Value: strPtr("url/hello/world")},
									{Key: LabelHTTPStatusCode, Value: intPtr(500)},
									{Key: "foo", Value: strPtr("def")},
									{Key: "bar", Value: intPtr(123)},
									{Key: "float", Value: fltPtr(456.78)},
									{Key: "bool", Value: boolPtr(false)},
									// Edge-cases
									{Key: LabelName, Value: strPtr("Bob")},                    // Conflicts with intrinsic but still looked up by .name
									{Key: LabelServiceName, Value: strPtr("spanservicename")}, // Overrides resource-level dedicated column
									{Key: LabelHTTPStatusCode, Value: strPtr("500ouch")},      // Different type than dedicated column
								},
								Links: []*v1.Span_Link{
									{
										TraceId:                make([]byte, 16),
										SpanId:                 make([]byte, 8),
										TraceState:             "state",
										DroppedAttributesCount: 3,
										Attributes: []*v1_common.KeyValue{
											{
												Key:   "key",
												Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_StringValue{StringValue: "value"}},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			{
				Resource: &v1_resource.Resource{
					Attributes: []*v1_common.KeyValue{
						{Key: LabelServiceName, Value: strPtr("service2")},
					},
				},
				ScopeSpans: []*v1.ScopeSpans{
					{
						Spans: []*v1.Span{
							{
								SpanId: []byte("spanid22"),
								Name:   "world",
							},
						},
					},
				},
			},
		},
	}
}
