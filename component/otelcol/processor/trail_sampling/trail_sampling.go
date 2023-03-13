package trail_sampling

import (
	"time"

	"github.com/grafana/agent/component"
	"github.com/grafana/agent/component/otelcol"
	"github.com/grafana/agent/component/otelcol/processor"
	trailprocessor "github.com/grafana/agent/component/otelcol/processor/trail_sampling/internal/processor"
	"github.com/grafana/agent/pkg/river"
	"github.com/mitchellh/mapstructure"
	otelcomponent "go.opentelemetry.io/collector/component"
	otelconfig "go.opentelemetry.io/collector/config"
)

func init() {
	component.Register(component.Registration{
		Name:    "otelcol.processor.trail_sampling",
		Args:    Arguments{},
		Exports: otelcol.ConsumerExports{},

		Build: func(opts component.Options, args component.Arguments) (component.Component, error) {
			fact := trailprocessor.NewFactory()
			return processor.New(opts, fact, args.(Arguments))
		},
	})
}

type PolicyCfg struct {
	// Name is the name of the policy. Required.
	Name string `river:"name,attr"`
	// Query is the query to use for the policy. Required.
	Query string `river:"query,attr"`
	// Probabilistic is the probability to sample a trace. Required.
	Probabilistic float64 `river:"probabilistic,attr"`
}

func (cfg *PolicyCfg) Convert() trailprocessor.PolicyCfg {
	return trailprocessor.PolicyCfg{
		Name:          cfg.Name,
		Query:         cfg.Query,
		Probabilistic: cfg.Probabilistic,
	}
}

// Arguments configures the otelcol.processor.traceql_sampling component.
type Arguments struct {
	DecisionWait            time.Duration `river:"decision_wait,attr,optional"`
	NumTraces               int           `river:"num_traces,attr,optional"`
	ExpectedNewTracesPerSec int           `river:"expected_new_traces_per_sec,attr,optional"`
	// Policies configures the sampling policies. Required.
	Policies []PolicyCfg `river:"policies,block"`

	// Output configures where to send processed data. Required.
	Output *otelcol.ConsumerArguments `river:"output,block"`
}

var (
	_ processor.Arguments = Arguments{}
	_ river.Unmarshaler   = (*Arguments)(nil)
)

// UnmarshalRiver implements river.Unmarshaler. It applies defaults to args and
// validates settings provided by the user.
func (args *Arguments) UnmarshalRiver(f func(interface{}) error) error {
	type arguments Arguments
	if err := f((*arguments)(args)); err != nil {
		return err
	}

	return nil
}

// Convert implements processor.Arguments.
func (args Arguments) Convert() otelconfig.Processor {
	var otelPolicyCfgs []trailprocessor.PolicyCfg
	for _, policyCfg := range args.Policies {
		otelPolicyCfgs = append(otelPolicyCfgs, policyCfg.Convert())
	}

	var tspCfg trailprocessor.Config
	mustDecodeMapStructure(map[string]interface{}{
		"decision_wait":               args.DecisionWait,
		"num_traces":                  args.NumTraces,
		"expected_new_traces_per_sec": args.ExpectedNewTracesPerSec,
		"policies":                    otelPolicyCfgs,
	}, &tspCfg)

	return nil
}

// Extensions implements processor.Arguments.
func (args Arguments) Extensions() map[otelconfig.ComponentID]otelcomponent.Extension { return nil }

// Exporters implements processor.Arguments.
func (args Arguments) Exporters() map[otelconfig.DataType]map[otelconfig.ComponentID]otelcomponent.Exporter {
	return nil
}

// NextConsumers implements processor.Arguments.
func (args Arguments) NextConsumers() *otelcol.ConsumerArguments { return args.Output }

func mustDecodeMapStructure(source map[string]interface{}, otelCfg interface{}) {
	err := mapstructure.Decode(source, otelCfg)

	if err != nil {
		panic(err)
	}
}
