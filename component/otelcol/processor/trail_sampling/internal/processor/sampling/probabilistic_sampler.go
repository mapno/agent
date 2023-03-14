package sampling

import (
	"hash/fnv"
	"math"
	"math/big"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

func NewProbabilisticSampler(ratio float64) *ProbabilisticSampler {
	return &ProbabilisticSampler{
		threshold: calculateThreshold(ratio),
	}
}

var _ PolicyEvaluator = (*ProbabilisticSampler)(nil)

type ProbabilisticSampler struct {
	threshold uint64
}

func (s *ProbabilisticSampler) Evaluate(traceID pcommon.TraceID, td *TraceData) (Decision, error) {
	// Short circuits
	if s.threshold == 0 {
		return NotSampled, nil
	}
	if s.threshold == math.MaxUint64 {
		return Sampled, nil
	}

	if hashTraceID("", traceID[:]) <= s.threshold {
		return Sampled, nil
	}

	return NotSampled, nil
}

// calculateThreshold converts a ratio into a value between 0 and MaxUint64
func calculateThreshold(ratio float64) uint64 {
	// Use big.Float and big.Int to calculate threshold because directly convert
	// math.MaxUint64 to float64 will cause digits/bits to be cut off if the converted value
	// doesn't fit into bits that are used to store digits for float64 in Golang
	boundary := new(big.Float).SetInt(new(big.Int).SetUint64(math.MaxUint64))
	res, _ := boundary.Mul(boundary, big.NewFloat(ratio)).Uint64()
	return res
}

// hashTraceID creates a hash using the FNV-1a algorithm.
func hashTraceID(_ string, b []byte) uint64 {
	hasher := fnv.New64a()
	// the implementation fnv.Write() never returns an error, see hash/fnv/fnv.go
	// _, _ = hasher.Write([]byte(salt))
	_, _ = hasher.Write(b)
	return hasher.Sum64()
}
