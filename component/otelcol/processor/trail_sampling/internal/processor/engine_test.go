package processor

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/grafana/tempo/pkg/traceql"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/collector/semconv/v1.13.0"
	v1_common "go.opentelemetry.io/proto/otlp/common/v1"
	v1_resource "go.opentelemetry.io/proto/otlp/resource/v1"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

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

func newSpanAttr(name string) traceql.Attribute {
	return traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, name)
}

func newResAttr(name string) traceql.Attribute {
	return traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, name)
}

func TestBackendBlockSearchTraceQL(t *testing.T) {

	ctx := context.TODO()
	wantTraceID := [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	wantTr, err := fromProto(fullyPopulatedTestTrace(wantTraceID))
	require.NoError(t, err)

	searchesThatMatch := []string{
		"{}", // Empty request

		// Intrinsics
		`{` + LabelName + ` = "hello"}`,
		`{` + LabelDuration + ` =  100s}`,
		`{` + LabelDuration + ` >  99s}`,
		`{` + LabelDuration + ` >= 100s}`,
		`{` + LabelDuration + ` <  101s}`,
		`{` + LabelDuration + ` <= 100s}`,
		`{` + LabelDuration + ` <= 100s}`,
		`{` + LabelStatus + ` = error}`,
		// Resource well-known attributes
		/*`{.` + LabelServiceName + ` = "spanservicename"}`, // Overridden at span
		`{.` + LabelCluster + ` = "cluster"}`,
		`{.` + LabelNamespace + ` = "namespace"}`,
		`{.` + LabelPod + ` = "pod"}`,
		`{.` + LabelContainer + ` = "container"}`,
		`{.` + LabelK8sNamespaceName + ` = "k8snamespace"}`,
		`{.` + LabelK8sClusterName + ` = "k8scluster"}`,
		`{.` + LabelK8sPodName + ` = "k8spod"}`,
		`{.` + LabelK8sContainerName + ` = "k8scontainer"}`,
		`{resource.` + LabelServiceName + ` = "myservice"}`,
		`{resource.` + LabelCluster + ` = "cluster"}`,
		`{resource.` + LabelNamespace + ` = "namespace"}`,
		`{resource.` + LabelPod + ` = "pod"}`,
		`{resource.` + LabelContainer + ` = "container"}`,
		`{resource.` + LabelK8sNamespaceName + ` = "k8snamespace"}`,
		`{resource.` + LabelK8sClusterName + ` = "k8scluster"}`,
		`{resource.` + LabelK8sPodName + ` = "k8spod"}`,
		`{resource.` + LabelK8sContainerName + ` = "k8scontainer"}`,
		// Span well-known attributes
		`{.` + LabelHTTPStatusCode + ` = 500}`,
		`{.` + LabelHTTPMethod + ` = "get"}`,
		`{.` + LabelHTTPUrl + ` = "url/hello/world"}`,
		`{span.` + LabelHTTPStatusCode + ` = 500}`,
		`{span.` + LabelHTTPMethod + ` = "get"}`,
		`{span.` + LabelHTTPUrl + ` = "url/hello/world"}`,
		// Basic data types and operations
		`{.float = 456.78}`,      // Float ==
		`{.float != 456.79}`,     // Float !=
		`{.float > 456.7}`,       // Float >
		`{.float >= 456.78}`,     // Float >=
		`{.float < 456.781}`,     // Float <
		`{.bool = false}`,        // Bool ==
		`{.bool != true}`,        // Bool !=
		`{.bar = 123}`,           // Int ==
		`{.bar != 124}`,          // Int !=
		`{.bar > 122}`,           // Int >
		`{.bar >= 123}`,          // Int >=
		`{.bar < 124}`,           // Int <
		`{.bar <= 123}`,          // Int <=
		`{.foo = "def"}`,         // String ==
		`{.foo != "deg"}`,        // String !=
		`{.foo =~ "d.*"}`,        // String Regex
		`{resource.foo = "abc"}`, // Resource-level only
		`{span.foo = "def"}`,     // Span-level only
		`{.foo}`,                 // Projection only
		`{.foo = "baz" || .` + LabelHTTPStatusCode + ` > 100}`,                       // Matches either condition
		`{.` + LabelHTTPStatusCode + ` > 100 || .foo = "baz"}`,                       // Same as above but reversed order
		`{.foo > 100 || .foo = "def"}`,                                               // Same attribute with mixed types
		`{.` + LabelHTTPStatusCode + ` = 500 || .` + LabelHTTPStatusCode + ` > 500}`, // Multiple conditions on same well-known attribute, matches either
		`{` + LabelName + ` = "hello" || ` + LabelDuration + ` < 100s }`,             // Mix of duration with other conditions
		`{.name = "Bob"}`, // Almost conflicts with intrinsic but still works
		`{resource.` + LabelServiceName + ` = 123}`,                                              // service.name doesn't match type of dedicated column
		`{.` + LabelServiceName + ` = "spanservicename"}`,                                        // service.name present on span
		`{.` + LabelHTTPStatusCode + ` = "500ouch"}`,                                             // http.status_code doesn't match type of dedicated column
		`{.` + LabelHTTPStatusCode + ` >= 500 && .` + LabelHTTPStatusCode + ` <= 600}`,           // Range at unscoped
		`{span.` + LabelHTTPStatusCode + ` >= 500 && span.` + LabelHTTPStatusCode + ` <= 600}`,   // Range at span scope
		`{resource.` + LabelServiceName + ` >= 122 && resource.` + LabelServiceName + ` <= 124}`, // Range at resource scope
		*/
	}

	for _, req := range searchesThatMatch {
		resp, err := Matches(ctx, wantTr, req)
		require.NoError(t, err, "search request:%v", req)
		require.True(t, resp, "search request: %+v", req)
	}

	searchesThatDontMatch := []string{
		// TODO - Should the below query return data or not?  It does match the resource
		// makeReq(parse(t, `{.foo = "abc"}`)),                           // This should not return results because the span has overridden this attribute to "def".
		/*`{.foo =~ "xyz.*"}`,                                                                             // Regex IN
		`{span.bool = true}`,                                                                            // Bool not match*/
		`{` + LabelDuration + ` >  100s}`,  // Intrinsic: duration
		`{` + LabelStatus + ` = ok}`,       // Intrinsic: status
		`{` + LabelName + ` = "nothello"}`, // Intrinsic: name
		/*`{.` + LabelServiceName + ` = "notmyservice"}`,                                                  // Well-known attribute: service.name not match
		`{.` + LabelHTTPStatusCode + ` = 200}`,                                                          // Well-known attribute: http.status_code not match
		`{.` + LabelHTTPStatusCode + ` > 600}`,                                                          // Well-known attribute: http.status_code not match
		`{.foo = "xyz" || .` + LabelHTTPStatusCode + " = 1000}",                                         // Matches neither condition
		`{span.foo = "baz" && span.` + LabelHTTPStatusCode + ` > 100 && name = "hello"}`,                // Matches some conditions but not all
		`{span.foo = "baz" && span.bar = 123}`,                                                          // Matches some conditions but not all
		`{resource.cluster = "cluster" && resource.namespace = "namespace" && span.foo = "baz"}`,        // Matches some conditions but not all
		`{resource.cluster = "notcluster" && resource.namespace = "namespace" && resource.foo = "abc"}`, // Matches some conditions but not all
		`{resource.foo = "abc" && resource.bar = 123}`,                                                  // Matches some conditions but not all*/
		`{` + LabelName + ` = "nothello" && ` + LabelDuration + ` = 100s }`, // Mix of duration with other conditions
	}

	for _, req := range searchesThatDontMatch {
		res, err := Matches(ctx, wantTr, req)
		require.NoError(t, err, "search request: %+v", req)
		require.False(t, res, "search request: %+v", req)
	}
}

func makeReq(conditions ...traceql.Condition) traceql.FetchSpansRequest {
	return traceql.FetchSpansRequest{
		Conditions: conditions,
	}
}

func parse(t *testing.T, q string) traceql.Condition {

	req, err := traceql.ExtractFetchSpansRequest(q)
	require.NoError(t, err, "query:", q)

	return req.Conditions[0]
}

func TestBackendBlockSearchFetchMetaData(t *testing.T) {
	ctx := context.Background()
	trID := [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	wantTr := fullyPopulatedTestTrace(trID)

	// Helper functions to make requests
	makeSpansets := func(sets ...*traceql.Spanset) []*traceql.Spanset {
		return sets
	}

	makeSpanset := func(spans ...traceql.Span) *traceql.Spanset {
		return &traceql.Spanset{
			TraceID: trID[:],
			Spans:   spans,
		}
	}

	testCases := []struct {
		req             traceql.FetchSpansRequest
		expectedResults []*traceql.Spanset
	}{
		{
			// Empty request returns 1 spanset with all spans
			traceql.FetchSpansRequest{},
			makeSpansets(
				makeSpanset(
					&span{
						id:                 wantTr.ResourceSpans[0].ScopeSpans[0].Spans[0].SpanId,
						startTimeUnixNanos: wantTr.ResourceSpans[0].ScopeSpans[0].Spans[0].StartTimeUnixNano,
						endtimeUnixNanos:   wantTr.ResourceSpans[0].ScopeSpans[0].Spans[0].EndTimeUnixNano,
						attributes:         map[traceql.Attribute]traceql.Static{},
					},
					&span{
						id:                 wantTr.ResourceSpans[1].ScopeSpans[0].Spans[0].SpanId,
						startTimeUnixNanos: wantTr.ResourceSpans[1].ScopeSpans[0].Spans[0].StartTimeUnixNano,
						endtimeUnixNanos:   wantTr.ResourceSpans[1].ScopeSpans[0].Spans[0].EndTimeUnixNano,
						attributes:         map[traceql.Attribute]traceql.Static{},
					},
				),
			),
		},
		/*{
			// Span attributes lookup
			// Only matches 1 condition. Returns span but only attributes that matched
			makeReq(
				parse(t, `{span.foo = "bar"}`), // matches resource but not span
				parse(t, `{span.bar = 123}`),   // matches
			),
			makeSpansets(
				makeSpanset(
					&span{
						id:                 wantTr.ResourceSpans[0].ScopeSpans[0].Spans[0].SpanId,
						startTimeUnixNanos: wantTr.ResourceSpans[0].ScopeSpans[0].Spans[0].StartTimeUnixNano,
						endtimeUnixNanos:   wantTr.ResourceSpans[0].ScopeSpans[0].Spans[0].EndTimeUnixNano,
						attributes: map[traceql.Attribute]traceql.Static{
							// foo not returned because the span didn't match it
							traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, "bar"): traceql.NewStaticInt(123),
						},
					},
				),
			),
		},

		{
			// Resource attributes lookup
			makeReq(
				parse(t, `{resource.foo = "abc"}`), // matches resource but not span
			),
			makeSpansets(
				makeSpanset(
					&span{
						id:                 wantTr.ResourceSpans[0].ScopeSpans[0].Spans[0].SpanId,
						startTimeUnixNanos: wantTr.ResourceSpans[0].ScopeSpans[0].Spans[0].StartTimeUnixNano,
						endtimeUnixNanos:   wantTr.ResourceSpans[0].ScopeSpans[0].Spans[0].EndTimeUnixNano,
						attributes: map[traceql.Attribute]traceql.Static{
							// Foo matched on resource.
							// TODO - This seems misleading since the span has foo=<something else>
							//        but for this query we never even looked at span attribute columns.
							newResAttr("foo"): traceql.NewStaticString("abc"),
						},
					},
				),
			),
		},

		{
			// Multiple attributes, only 1 matches and is returned
			makeReq(
				parse(t, `{.foo = "xyz"}`),                   // doesn't match anything
				parse(t, `{.`+LabelHTTPStatusCode+` = 500}`), // matches span
			),
			makeSpansets(
				makeSpanset(
					&span{
						id:                 wantTr.ResourceSpans[0].ScopeSpans[0].Spans[0].SpanId,
						startTimeUnixNanos: wantTr.ResourceSpans[0].ScopeSpans[0].Spans[0].StartTimeUnixNano,
						endtimeUnixNanos:   wantTr.ResourceSpans[0].ScopeSpans[0].Spans[0].EndTimeUnixNano,
						attributes: map[traceql.Attribute]traceql.Static{
							newSpanAttr(LabelHTTPStatusCode): traceql.NewStaticInt(500), // This is the only attribute that matched anything
						},
					},
				),
			),
		},

		{
			// Project attributes of all types
			makeReq(
				parse(t, `{.foo }`),                    // String
				parse(t, `{.`+LabelHTTPStatusCode+`}`), // Int
				parse(t, `{.float }`),                  // Float
				parse(t, `{.bool }`),                   // bool
			),
			makeSpansets(
				makeSpanset(
					&span{
						id:                 wantTr.ResourceSpans[0].ScopeSpans[0].Spans[0].SpanId,
						startTimeUnixNanos: wantTr.ResourceSpans[0].ScopeSpans[0].Spans[0].StartTimeUnixNano,
						endtimeUnixNanos:   wantTr.ResourceSpans[0].ScopeSpans[0].Spans[0].EndTimeUnixNano,
						attributes: map[traceql.Attribute]traceql.Static{
							newResAttr("foo"):                traceql.NewStaticString("abc"), // Both are returned
							newSpanAttr("foo"):               traceql.NewStaticString("def"), // Both are returned
							newSpanAttr(LabelHTTPStatusCode): traceql.NewStaticInt(500),
							newSpanAttr("float"):             traceql.NewStaticFloat(456.78),
							newSpanAttr("bool"):              traceql.NewStaticBool(false),
						},
					},
				),
			),
		},*/

		{
			// doesn't match anything
			makeReq(parse(t, `{.xyz = "xyz"}`)),
			nil,
		},

		{
			// Intrinsics. 2nd span only
			makeReq(
				parse(t, `{ name = "world" }`),
				parse(t, `{ status = unset }`),
			),
			makeSpansets(
				makeSpanset(
					&span{
						id:                 wantTr.ResourceSpans[1].ScopeSpans[0].Spans[0].SpanId,
						startTimeUnixNanos: wantTr.ResourceSpans[1].ScopeSpans[0].Spans[0].StartTimeUnixNano,
						endtimeUnixNanos:   wantTr.ResourceSpans[1].ScopeSpans[0].Spans[0].EndTimeUnixNano,
						attributes: map[traceql.Attribute]traceql.Static{
							traceql.NewIntrinsic(traceql.IntrinsicName):   traceql.NewStaticString("world"),
							traceql.NewIntrinsic(traceql.IntrinsicStatus): traceql.NewStaticStatus(traceql.StatusUnset),
						},
					},
				),
			),
		},
		/*{
			// Intrinsic duration with no filtering
			traceql.FetchSpansRequest{Conditions: []traceql.Condition{{Attribute: traceql.NewIntrinsic(traceql.IntrinsicDuration)}}},
			makeSpansets(
				makeSpanset(
					&span{
						id:                 wantTr.ResourceSpans[0].ScopeSpans[0].Spans[0].SpanId,
						startTimeUnixNanos: wantTr.ResourceSpans[0].ScopeSpans[0].Spans[0].StartTimeUnixNano,
						endtimeUnixNanos:   wantTr.ResourceSpans[0].ScopeSpans[0].Spans[0].EndTimeUnixNano,
						attributes: map[traceql.Attribute]traceql.Static{
							traceql.NewIntrinsic(traceql.IntrinsicDuration): traceql.NewStaticDuration(100 * time.Second),
						},
					},
					&span{
						id:                 wantTr.ResourceSpans[1].ScopeSpans[0].Spans[0].SpanId,
						startTimeUnixNanos: wantTr.ResourceSpans[1].ScopeSpans[0].Spans[0].StartTimeUnixNano,
						endtimeUnixNanos:   wantTr.ResourceSpans[1].ScopeSpans[0].Spans[0].EndTimeUnixNano,
						attributes: map[traceql.Attribute]traceql.Static{
							traceql.NewIntrinsic(traceql.IntrinsicDuration): traceql.NewStaticDuration(0 * time.Second),
						},
					},
				),
			),
		},*/
	}

	for _, tc := range testCases {

		tr, err := fromProto(fullyPopulatedTestTrace(trID))
		require.NoError(t, err)

		b := &PtraceFetcher{
			td: tr,
		}

		req := tc.req
		resp, err := b.Fetch(ctx, req)
		require.NoError(t, err, "search request: %+v", tc.req)
		require.NotNil(t, resp, "search request: %+v", tc.req)
		require.NotNil(t, resp.Results, "search request: %+v", tc.req)

		// Turn iterator into slice
		var ss []*traceql.Spanset
		for {
			spanSet, err := resp.Results.Next(ctx)
			require.NoError(t, err, "search request: %+v", tc.req)
			if spanSet == nil {
				break
			}
			ss = append(ss, spanSet)
		}

		require.Equal(t, tc.expectedResults, ss, "search request: %+v", tc.req)
	}
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
						{Key: LabelServiceName, Value: intPtr(123)}, // Different type than dedicated column
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

func buildSampleTrace(attrValue string) ptrace.Traces {
	tStart := time.Date(2022, 1, 2, 3, 4, 5, 6, time.UTC)
	tEnd := time.Date(2022, 1, 2, 3, 4, 6, 6, time.UTC)

	traces := ptrace.NewTraces()

	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr(semconv.AttributeServiceName, "some-service")

	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()

	var traceID pcommon.TraceID
	rand.Read(traceID[:])

	var clientSpanID, serverSpanID pcommon.SpanID
	rand.Read(clientSpanID[:])
	rand.Read(serverSpanID[:])

	clientSpan := scopeSpans.Spans().AppendEmpty()
	clientSpan.SetName("client span")
	clientSpan.SetSpanID(clientSpanID)
	clientSpan.SetTraceID(traceID)
	clientSpan.SetKind(ptrace.SpanKindClient)
	clientSpan.SetStartTimestamp(pcommon.NewTimestampFromTime(tStart))
	clientSpan.SetEndTimestamp(pcommon.NewTimestampFromTime(tEnd))
	clientSpan.Attributes().PutStr("some-attribute", attrValue) // Attribute selected as dimension for metrics

	serverSpan := scopeSpans.Spans().AppendEmpty()
	serverSpan.SetName("server span")
	serverSpan.SetSpanID(serverSpanID)
	serverSpan.SetTraceID(traceID)
	serverSpan.SetParentSpanID(clientSpanID)
	serverSpan.SetKind(ptrace.SpanKindServer)
	serverSpan.SetStartTimestamp(pcommon.NewTimestampFromTime(tStart))
	serverSpan.SetEndTimestamp(pcommon.NewTimestampFromTime(tEnd))

	return traces
}
