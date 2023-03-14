package processor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/spanmetricsprocessor/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestTrailSamplingProcessor_HappyPath(t *testing.T) {
	cfg := Config{
		DecisionWait: 10 * time.Second,
	}
	mockConsumer := &mocks.TracesConsumer{}
	mockConsumer.On("ConsumeTraces").Run(func(args mock.Arguments) {
		fmt.Println(args)
	}).Return(nil)
	s, err := newTracesProcessor(context.TODO(), mockConsumer, cfg)
	assert.NoError(t, err)

	mockHost := &mocks.Host{}
	assert.NoError(t, s.Start(context.TODO(), mockHost))

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("foo", "bar")
	assert.NoError(t, s.ConsumeTraces(context.TODO(), td))
}
