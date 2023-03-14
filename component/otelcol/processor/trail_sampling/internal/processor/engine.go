package processor

import (
	"context"
	"encoding/hex"
	"regexp"
	"time"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

var engine = traceql.NewEngine()

func Matches(ctx context.Context, trace ptrace.Traces, query string) (bool, error) {
	f := &PtraceFetcher{td: trace}
	req := &tempopb.SearchRequest{
		Query: query,
	}
	res, err := engine.Execute(ctx, req, f)
	if err != nil {
		return false, err
	}
	return len(res.Traces) > 0, nil
}

type PtraceFetcher struct {
	td ptrace.Traces
}

var _ traceql.SpansetFetcher = (*PtraceFetcher)(nil)

// Fetch - Implements a traceql storage backend that runs against the in-memory trace.
// It is a minimal implementation and returns spans that a match at least 1 condition.
// Does not honor the AllConditions hint.
func (f *PtraceFetcher) Fetch(_ context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
	var matchingSpans []traceql.Span
	var traceID []byte
	var err error
	td := f.td

	// Split all conditions into appropriate scope
	var resourceConditions []traceql.Condition
	var spanConditions []traceql.Condition
	for _, c := range req.Conditions {
		switch c.Attribute.Scope {
		case traceql.AttributeScopeSpan:
			spanConditions = append(spanConditions, c)
		case traceql.AttributeScopeResource:
			resourceConditions = append(resourceConditions, c)
		case traceql.AttributeScopeNone:
			resourceConditions = append(resourceConditions, c)
			spanConditions = append(spanConditions, c)
		}
	}

	resourceSpans := td.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {

		rs := resourceSpans.At(i)

		res := rs.Resource()
		matchedAttrsRes := matchResource(resourceConditions, res)

		scopeSpans := rs.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			spans := scopeSpans.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				s := spans.At(k)

				if traceID == nil {
					traceID = mustDecodeString(s.TraceID().HexString())
				}

				start, end := s.StartTimestamp().AsTime().UnixNano(), s.EndTimestamp().AsTime().UnixNano()
				spanID := mustDecodeString(s.SpanID().HexString())

				if len(req.Conditions) == 0 {
					// No conditions is a match
					matchingSpans = append(matchingSpans, &span{
						id:                 spanID,
						startTimeUnixNanos: uint64(start),
						endtimeUnixNanos:   uint64(end),
						attributes:         make(map[traceql.Attribute]traceql.Static),
					})
					continue
				}

				matchedAttrs := matchSpan(spanConditions, s)

				// Anything?
				if len(matchedAttrsRes) > 0 || len(matchedAttrs) > 0 {
					appendMatches(matchedAttrs, matchedAttrsRes)
					matchingSpans = append(matchingSpans, &span{
						id:                 spanID,
						startTimeUnixNanos: uint64(start),
						endtimeUnixNanos:   uint64(end),
						attributes:         matchedAttrs,
					})
				}
			}
		}
	}

	if len(matchingSpans) == 0 {
		return traceql.FetchSpansResponse{Results: &TraceProtoResults{
			results: nil,
		}}, nil
	}

	ss := &traceql.Spanset{
		TraceID: traceID,
		Spans:   matchingSpans,
	}

	var filteredSpansets []*traceql.Spanset

	if req.Filter != nil {
		filteredSpansets, err = req.Filter(ss)
		if err != nil {
			return traceql.FetchSpansResponse{}, err
		}
	} else {
		filteredSpansets = []*traceql.Spanset{ss}
	}

	return traceql.FetchSpansResponse{
		Results: &TraceProtoResults{
			results: filteredSpansets,
		},
	}, nil
}

func matchResource(conditions []traceql.Condition, res pcommon.Resource) map[traceql.Attribute]traceql.Static {
	output := map[traceql.Attribute]traceql.Static{}

	// There are no intrinsics on resource, so we just
	// check attributes.
	for _, c := range conditions {
		if v := matchAttr(c, res.Attributes()); v.Type != traceql.TypeNil {
			output[traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, c.Attribute.Name)] = v
		}
	}

	return output
}

func matchSpan(conditions []traceql.Condition, span ptrace.Span) map[traceql.Attribute]traceql.Static {
	output := map[traceql.Attribute]traceql.Static{}

	for _, c := range conditions {
		if c.Attribute.Intrinsic != traceql.IntrinsicNone {
			switch c.Attribute.Intrinsic {
			case traceql.IntrinsicDuration:
				d := time.Duration(span.EndTimestamp() - span.StartTimestamp())
				if evalDuration(c, d) {
					output[c.Attribute] = traceql.NewStaticDuration(d)
				}
			case traceql.IntrinsicName:
				if evalStr(c, span.Name()) {
					output[c.Attribute] = traceql.NewStaticString(span.Name())
				}
			case traceql.IntrinsicStatus:
				st := statusPtraceToTraceQL(span.Status())
				if evalStatus(c, st) {
					output[c.Attribute] = traceql.NewStaticStatus(st)
				}
			}
			continue
		}

		// Check generic attributes map
		if v := matchAttr(c, span.Attributes()); v.Type != traceql.TypeNil {
			output[traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, c.Attribute.Name)] = v
		}
	}
	return output
}

func matchAttr(c traceql.Condition, attrs pcommon.Map) traceql.Static {
	if a, ok := attrs.Get(c.Attribute.Name); ok {
		switch a.Type() {
		case pcommon.ValueTypeStr:
			if evalStr(c, a.Str()) {
				return traceql.NewStaticString(a.Str())
			}
		case pcommon.ValueTypeInt:
			if evalInt(c, int(a.Int())) {
				return traceql.NewStaticInt(int(a.Int()))
			}
		case pcommon.ValueTypeDouble:
			if evalFloat(c, a.Double()) {
				return traceql.NewStaticFloat(a.Double())
			}
		case pcommon.ValueTypeBool:
			if evalBool(c, a.Bool()) {
				return traceql.NewStaticBool(a.Bool())
			}
		}
	}
	return traceql.NewStaticNil()
}

func evalStr(c traceql.Condition, s string) bool {
	if c.Op == traceql.OpNone {
		return true
	}
	op := c.Operands[0].S
	switch c.Op {
	case traceql.OpEqual:
		return s == op
	case traceql.OpNotEqual:
		return s != op
	case traceql.OpRegex:
		if reg, err := regexp.Compile(op); err == nil {
			return reg.MatchString(s)
		}
	case traceql.OpNotRegex:
		if reg, err := regexp.Compile(op); err == nil {
			return !reg.MatchString(s)
		}
	}
	return false
}

func evalInt(c traceql.Condition, i int) bool {
	if c.Op == traceql.OpNone {
		return true
	}
	op := c.Operands[0].N
	switch c.Op {
	case traceql.OpEqual:
		return i == op
	case traceql.OpNotEqual:
		return i != op
	case traceql.OpGreater:
		return i > op
	case traceql.OpGreaterEqual:
		return i >= op
	case traceql.OpLess:
		return i < op
	case traceql.OpLessEqual:
		return i <= op
	}
	return false
}

func evalFloat(c traceql.Condition, i float64) bool {
	if c.Op == traceql.OpNone {
		return true
	}
	op := c.Operands[0].F
	switch c.Op {
	case traceql.OpEqual:
		return i == op
	case traceql.OpNotEqual:
		return i != op
	case traceql.OpGreater:
		return i > op
	case traceql.OpGreaterEqual:
		return i >= op
	case traceql.OpLess:
		return i < op
	case traceql.OpLessEqual:
		return i <= op
	}
	return false
}

func evalBool(c traceql.Condition, i bool) bool {
	if c.Op == traceql.OpNone {
		return true
	}
	op := c.Operands[0].B
	switch c.Op {
	case traceql.OpEqual:
		return i == op
	case traceql.OpNotEqual:
		return i != op
	}
	return false
}

func evalDuration(c traceql.Condition, i time.Duration) bool {
	if c.Op == traceql.OpNone {
		return true
	}
	op := c.Operands[0].D
	switch c.Op {
	case traceql.OpEqual:
		return i == op
	case traceql.OpNotEqual:
		return i != op
	case traceql.OpGreater:
		return i > op
	case traceql.OpGreaterEqual:
		return i >= op
	case traceql.OpLess:
		return i < op
	case traceql.OpLessEqual:
		return i <= op
	}
	return false
}

func evalStatus(c traceql.Condition, st traceql.Status) bool {
	if c.Op == traceql.OpNone {
		return true
	}
	op := c.Operands[0].Status
	switch c.Op {
	case traceql.OpEqual:
		return st == op
	case traceql.OpNotEqual:
		return st != op
	}
	return false
}

func appendMatches(to, from map[traceql.Attribute]traceql.Static) {
	for k, v := range from {
		to[k] = v
	}
}

func mustDecodeString(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func fromProto(trace *v1.TracesData) (ptrace.Traces, error) {
	buf, err := proto.Marshal(trace)
	if err != nil {
		return ptrace.Traces{}, err
	}

	u := &ptrace.ProtoUnmarshaler{}
	return u.UnmarshalTraces(buf)
}

type TraceProtoResults struct {
	results []*traceql.Spanset
}

var _ traceql.SpansetIterator = (*TraceProtoResults)(nil)

func (r *TraceProtoResults) Next(context.Context) (*traceql.Spanset, error) {
	if len(r.results) == 0 {
		return nil, nil
	}

	res := r.results[0]
	r.results = r.results[1:]
	return res, nil
}
func (r *TraceProtoResults) Close() {
}

type span struct {
	attributes         map[traceql.Attribute]traceql.Static
	id                 []byte
	startTimeUnixNanos uint64
	endtimeUnixNanos   uint64
}

var _ traceql.Span = (*span)(nil)

func (s *span) Attributes() map[traceql.Attribute]traceql.Static {
	return s.attributes
}
func (s *span) ID() []byte {
	return s.id
}
func (s *span) StartTimeUnixNanos() uint64 {
	return s.startTimeUnixNanos
}
func (s *span) EndtimeUnixNanos() uint64 {
	return s.endtimeUnixNanos
}
func (s *span) Release() {
}

func statusPtraceToTraceQL(status ptrace.Status) traceql.Status {
	switch status.Code() {
	case ptrace.StatusCodeOk:
		return traceql.StatusOk
	case ptrace.StatusCodeError:
		return traceql.StatusError
	}
	return traceql.StatusUnset
}

func statusProtoToTraceQL(status ptrace.Status) traceql.Status {
	switch status.Code() {
	case ptrace.StatusCodeError:
		return traceql.StatusError
	case ptrace.StatusCodeOk:
		return traceql.StatusOk
	}

	return traceql.StatusUnset
}
