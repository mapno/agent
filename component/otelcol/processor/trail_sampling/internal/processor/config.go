package processor

import (
	"fmt"
	"time"

	otelconfig "go.opentelemetry.io/collector/config"
)

type PolicyCfg struct {
	// Name is the name of the policy.
	Name string `mapstructure:"name"`
	// Query is the TraceQL query that will be used to sample traces.
	Query string `mapstructure:"query"`
	// SamplingRate is the probability of sampling a trace that matches the query.
	SamplingRate float64 `mapstructure:"sampling_rate"`
	// TracesPerSecond is the maximum number of traces per second that will be sampled.
	TracesPerSecond float64 `mapstructure:"traces_per_second`
}

func (p *PolicyCfg) Validate() error {
	if p.Name == "" {
		return fmt.Errorf("empty policy name")
	}
	if p.Query == "" {
		return fmt.Errorf("empty policy query. Use `{}` to match all traces")
	}
	if p.SamplingRate < 0 || p.SamplingRate > 1 {
		return fmt.Errorf("invalid sampling rate: %f. Must be between 0 and 1", p.SamplingRate)
	}
	if p.TracesPerSecond < 0 {
		return fmt.Errorf("invalid traces per second: %f. Must be greater than 0", p.TracesPerSecond)
	}
	if p.TracesPerSecond > 0 && p.SamplingRate > 0 {
		return fmt.Errorf("invalid policy: traces per second and sampling rate are mutually exclusive")
	}
	return nil
}

type Config struct {
	otelconfig.ProcessorSettings

	// DecisionWait is the desired wait time from the arrival of the first span of
	// trace until the decision about sampling it or not is evaluated.
	DecisionWait time.Duration `mapstructure:"decision_wait"`
	// NumTraces is the number of traces kept on memory. Typically most of the data
	// of a trace is released after a sampling decision is taken.
	NumTraces int `mapstructure:"num_traces"`
	// ExpectedNewTracesPerSec sets the expected number of new traces sending to the tail sampling processor
	// per second. This helps with allocating data structures with closer to actual usage size.
	ExpectedNewTracesPerSec uint64 `mapstructure:"expected_new_traces_per_sec"`
	// PolicyCfgs sets the tail-based sampling policy which makes a sampling decision
	// for a given trace when requested.
	PolicyCfgs []PolicyCfg `mapstructure:"policies"`
}

func (c *Config) Validate() error {
	if c.DecisionWait <= 0 {
		return fmt.Errorf("invalid decision wait: %v. Must be greater than 0", c.DecisionWait)
	}
	if c.NumTraces <= 0 {
		return fmt.Errorf("invalid num traces: %d. Must be greater than 0", c.NumTraces)
	}
	if c.ExpectedNewTracesPerSec <= 0 {
		return fmt.Errorf("invalid expected new traces per sec: %d. Must be greater than 0", c.ExpectedNewTracesPerSec)
	}
	for _, p := range c.PolicyCfgs {
		if err := p.Validate(); err != nil {
			return err
		}
	}
	return nil
}
