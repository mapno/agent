package trail_sampling

import (
	"fmt"
	"testing"
	"time"

	"github.com/grafana/agent/component/otelcol"
)

func TestArguments_Convert(t *testing.T) {
	args := &Arguments{
		DecisionWait:            20 * time.Second,
		NumTraces:               20,
		ExpectedNewTracesPerSec: 2,
		Policies: []PolicyCfg{
			{
				Name:         "policy1",
				Query:        "query1",
				SamplingRate: 0.1,
			},
		},
		Output: &otelcol.ConsumerArguments{},
	}
	otelCfg := args.Convert()
	fmt.Println(otelCfg.ID())
}
