package processor

import (
	"context"
	"time"

	otelcomponent "go.opentelemetry.io/collector/component"
	otelconfig "go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
)

func NewFactory() otelcomponent.ProcessorFactory {
	return otelcomponent.NewProcessorFactory(
		"trail_sampling",
		createDefaultConfig,
		otelcomponent.WithTracesProcessor(createTracesProcessor, otelcomponent.StabilityLevelAlpha),
	)
}

func createDefaultConfig() otelconfig.Processor {
	return &Config{
		ProcessorSettings: otelconfig.NewProcessorSettings(otelconfig.NewComponentID("trail_sampling")),
		DecisionWait:      30 * time.Second,
		NumTraces:         50000,
	}
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
