package processor

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"go.opentelemetry.io/collector/pdata/ptrace"
	v1 "go.opentelemetry.io/proto/otlp/trace/v1"
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

func (f *PtraceFetcher) Fetch(_ context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
	var matchingSpans []traceql.Span
	var traceID []byte
	var err error
	td := f.td

	resourceSpans := td.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		scopeSpans := resourceSpans.At(i).ScopeSpans()
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

				matchedAttrs := make(map[traceql.Attribute]traceql.Static)
				for _, cond := range req.Conditions {
					switch cond.Attribute {
					case traceql.NewIntrinsic(traceql.IntrinsicDuration):
						d := time.Duration(end - start)
						var dmatch bool
						switch cond.Op {
						case traceql.OpEqual:
							dmatch = d == cond.Operands[0].D
						case traceql.OpGreater:
							dmatch = d > cond.Operands[0].D
						case traceql.OpGreaterEqual:
							dmatch = d >= cond.Operands[0].D
						case traceql.OpLess:
							dmatch = d < cond.Operands[0].D
						case traceql.OpLessEqual:
							dmatch = d <= cond.Operands[0].D

						}
						if dmatch {
							matchedAttrs[cond.Attribute] = traceql.NewStaticDuration(d)
						}
					case traceql.NewIntrinsic(traceql.IntrinsicName):
						switch cond.Op {
						case traceql.OpEqual:
							if s.Name() == cond.Operands[0].S {
								matchedAttrs[cond.Attribute] = traceql.NewStaticString(s.Name())
							}
						}
					case traceql.NewIntrinsic(traceql.IntrinsicStatus):
						st := statusPtraceToTraceQL(s.Status())
						switch cond.Op {
						case traceql.OpEqual:
							if st == cond.Operands[0].Status {
								matchedAttrs[cond.Attribute] = traceql.NewStaticStatus(st)
							}
						}
					}
				}
				if len(matchedAttrs) > 0 {
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

func mustDecodeString(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func MatchesProto(ctx context.Context, trace *v1.TracesData, query string) (bool, error) {
	f := &TraceProtoFetcher{tr: trace}
	req := &tempopb.SearchRequest{
		Query: query,
	}
	res, err := engine.Execute(ctx, req, f)
	if err != nil {
		return false, err
	}
	return len(res.Traces) > 0, nil
}

type TraceProtoFetcher struct {
	tr *v1.TracesData
}

var _ traceql.SpansetFetcher = (*TraceProtoFetcher)(nil)

func (f *TraceProtoFetcher) Fetch(_ context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {

	var matchingSpans []traceql.Span
	var traceID []byte
	var err error
	trace := f.tr

	for _, rs := range trace.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			for _, s := range ss.Spans {

				if traceID == nil {
					traceID = s.TraceId
				}

				if len(req.Conditions) == 0 {
					// No conditions is a match
					matchingSpans = append(matchingSpans, &span{
						id:                 s.SpanId,
						startTimeUnixNanos: s.StartTimeUnixNano,
						endtimeUnixNanos:   s.EndTimeUnixNano,
						attributes:         make(map[traceql.Attribute]traceql.Static),
					})
					continue
				}

				matchedAttrs := make(map[traceql.Attribute]traceql.Static)
				for _, cond := range req.Conditions {
					switch cond.Attribute {
					case traceql.NewIntrinsic(traceql.IntrinsicDuration):
						d := time.Duration(s.EndTimeUnixNano - s.StartTimeUnixNano)
						var dmatch bool
						switch cond.Op {
						case traceql.OpEqual:
							dmatch = d == cond.Operands[0].D
						case traceql.OpGreater:
							dmatch = d > cond.Operands[0].D
						case traceql.OpGreaterEqual:
							dmatch = d >= cond.Operands[0].D
						case traceql.OpLess:
							dmatch = d < cond.Operands[0].D
						case traceql.OpLessEqual:
							dmatch = d <= cond.Operands[0].D

						}
						if dmatch {
							matchedAttrs[cond.Attribute] = traceql.NewStaticDuration(d)
						}
					case traceql.NewIntrinsic(traceql.IntrinsicName):
						switch cond.Op {
						case traceql.OpEqual:
							if s.Name == cond.Operands[0].S {
								matchedAttrs[cond.Attribute] = traceql.NewStaticString(s.Name)
							}
						}
					case traceql.NewIntrinsic(traceql.IntrinsicStatus):
						st := statusProtoToTraceQL(s.Status)
						switch cond.Op {
						case traceql.OpEqual:
							if st == cond.Operands[0].Status {
								matchedAttrs[cond.Attribute] = traceql.NewStaticStatus(st)
							}
						}
					}
				}
				if len(matchedAttrs) > 0 {
					matchingSpans = append(matchingSpans, &span{
						id:                 s.SpanId,
						startTimeUnixNanos: s.StartTimeUnixNano,
						endtimeUnixNanos:   s.EndTimeUnixNano,
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

func statusProtoToTraceQL(status *v1.Status) traceql.Status {
	if status != nil {
		switch status.Code {
		case v1.Status_STATUS_CODE_ERROR:
			return traceql.StatusError
		case v1.Status_STATUS_CODE_OK:
			return traceql.StatusOk
		}
	}

	return traceql.StatusUnset
}
