package processor

import (
	"context"

	otelcomponent "go.opentelemetry.io/collector/component"
	otelconfig "go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
)

type Config struct {
	otelconfig.ProcessorSettings
}

func NewFactory() otelcomponent.ProcessorFactory {
	return otelcomponent.NewProcessorFactory(
		"traceql",
		func() otelconfig.Processor {
			return &Config{}
		},
		otelcomponent.WithTracesProcessor(createTracesProcessor, otelcomponent.StabilityLevelAlpha),
	)
}

func createTracesProcessor(
	_ context.Context,
	params otelcomponent.ProcessorCreateSettings,
	cfg otelconfig.Processor,
	nextConsumer consumer.Traces,
) (otelcomponent.TracesProcessor, error) {
	tCfg := cfg.(*Config)
	return newProcessor(tCfg)
}
