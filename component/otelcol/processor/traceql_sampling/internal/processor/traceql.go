package processor

import (
	"context"

	otelcomponent "go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func newProcessor(cfg *Config) (otelcomponent.TracesProcessor, error) {
	return &processor{cfg: cfg}, nil
}

type processor struct {
	cfg *Config
}

func (p *processor) Start(ctx context.Context, _ otelcomponent.Host) error { return nil }

func (p *processor) Shutdown(ctx context.Context) error { return nil }

func (p *processor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	// TODO: Sample traces
	return nil
}

func (p *processor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}
