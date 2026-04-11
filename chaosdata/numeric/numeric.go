package numeric

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

type NumericGenerator struct{}

func (NumericGenerator) Name() string {
	return "numeric"
}

func (NumericGenerator) Category() string {
	return "numeric"
}

func (g NumericGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	records := []map[string]interface{}{}
	records = append(records, map[string]interface{}{"type": "Zero", "value": 0})
	records = append(records, map[string]interface{}{"type": "Negative Zero", "value": math.Copysign(0, -1)})
	records = append(records, map[string]interface{}{"type": "MaxInt64", "value": int64(math.MaxInt64)})
	records = append(records, map[string]interface{}{"type": "MinInt64", "value": int64(math.MinInt64)})
	records = append(records, map[string]interface{}{"type": "MaxFloat64", "value": math.MaxFloat64})
	records = append(records, map[string]interface{}{"type": "SmallestNonzeroFloat64", "value": math.SmallestNonzeroFloat64})
	records = append(records, map[string]interface{}{"type": "NaN", "value": "NaN"}) // JSON doesn't support NaN
	records = append(records, map[string]interface{}{"type": "+Inf", "value": "+Inf"})
	records = append(records, map[string]interface{}{"type": "-Inf", "value": "-Inf"})
	records = append(records, map[string]interface{}{"type": "MaxInt32+1", "value": int64(math.MaxInt32) + 1})
	records = append(records, map[string]interface{}{"type": "High-precision float", "value": 0.1234567890123456789})

	count := opts.Count
	if count < 1 {
		count = 1
	}

	all := make([]map[string]interface{}, 0, len(records)*count)
	for i := 0; i < count; i++ {
		all = append(all, records...)
	}

	data, err := json.Marshal(all)
	if err != nil {
		return chaosdata.Payload{}, fmt.Errorf("numeric: marshal payload: %w", err)
	}

	return chaosdata.Payload{
		Data: data,
		Type: "application/json",
		Attributes: map[string]string{
			"generator": g.Name(),
			"category":  g.Category(),
			"records":   fmt.Sprintf("%d", len(all)),
		},
	}, nil
}

func init() {
	chaosdata.Register(NumericGenerator{})
}
