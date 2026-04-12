package gospecific

import (
	"encoding/json"
	"fmt"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

type GoSpecificGenerator struct{}

func (GoSpecificGenerator) Name() string {
	return "gospecific"
}

func (GoSpecificGenerator) Category() string {
	return "gospecific"
}

func (g GoSpecificGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	records := []map[string]interface{}{}
	records = append(records, map[string]interface{}{"type": "Nil interface vs nil pointer-in-interface", "value": nil})
	records = append(records, map[string]interface{}{"type": "Unexported field struct", "value": struct{ hidden int }{1}})
	records = append(records, map[string]interface{}{"type": "context.Canceled error value", "value": "context canceled"})
	records = append(records, map[string]interface{}{"type": "context.DeadlineExceeded error value", "value": "context deadline exceeded"})
	records = append(records, map[string]interface{}{"type": "Unbuffered chan with no reader", "value": "chan int"})
	records = append(records, map[string]interface{}{"type": "time.Time{} zero value", "value": "0001-01-01T00:00:00Z"})
	records = append(records, map[string]interface{}{"type": "time.Unix(0,0)", "value": "1970-01-01T00:00:00Z"})
	records = append(records, map[string]interface{}{"type": "String with len>0 but only null bytes", "value": "\x00\x00\x00"})

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
		return chaosdata.Payload{}, fmt.Errorf("gospecific: marshal payload: %w", err)
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
	chaosdata.Register(GoSpecificGenerator{})
}
