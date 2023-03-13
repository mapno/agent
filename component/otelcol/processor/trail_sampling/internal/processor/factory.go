package processor

import (
	"context"

	otelcomponent "go.opentelemetry.io/collector/component"
	otelconfig "go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
)

func NewFactory() otelcomponent.ProcessorFactory {
	return otelcomponent.NewProcessorFactory(
		"trail_sampling",
		func() otelconfig.Processor {
			return &Config{}
		},
		otelcomponent.WithTracesProcessor(createTracesProcessor, otelcomponent.StabilityLevelAlpha),
	)
}

func createTracesProcessor(
	ctx context.Context,
	_ otelcomponent.ProcessorCreateSettings,
	cfg otelconfig.Processor,
	nextConsumer consumer.Traces,
) (otelcomponent.TracesProcessor, error) {
	tCfg := cfg.(*Config)
	return newTracesProcessor(ctx, nextConsumer, *tCfg)
}
