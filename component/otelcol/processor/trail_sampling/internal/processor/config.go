package processor

import (
	"time"

	otelconfig "go.opentelemetry.io/collector/config"
)

type PolicyCfg struct {
	// Name is the name of the policy.
	Name string `mapstructure:"name"`
	// Query is the TraceQL query that will be used to sample traces.
	Query string `mapstructure:"query"`
	// Probabilistic is the probability of sampling a trace that matches the query.
	Probabilistic float64 `mapstructure:"probabilistic"`
}

type Config struct {
	otelconfig.ProcessorSettings

	// DecisionWait is the desired wait time from the arrival of the first span of
	// trace until the decision about sampling it or not is evaluated.
	DecisionWait time.Duration `mapstructure:"decision_wait"`
	// NumTraces is the number of traces kept on memory. Typically most of the data
	// of a trace is released after a sampling decision is taken.
	NumTraces uint64 `mapstructure:"num_traces"`
	// ExpectedNewTracesPerSec sets the expected number of new traces sending to the tail sampling processor
	// per second. This helps with allocating data structures with closer to actual usage size.
	ExpectedNewTracesPerSec uint64 `mapstructure:"expected_new_traces_per_sec"`
	// PolicyCfgs sets the tail-based sampling policy which makes a sampling decision
	// for a given trace when requested.
	PolicyCfgs []PolicyCfg `mapstructure:"policies"`
}
