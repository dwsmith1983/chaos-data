package volume

import (
	"encoding/json"
	"fmt"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

type VolumeGenerator struct{}

func (VolumeGenerator) Name() string {
	return "volume"
}

func (VolumeGenerator) Category() string {
	return "volume"
}

func (g VolumeGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	records := []map[string]interface{}{}
	records = append(records, map[string]interface{}{"type": "1MB string (materialized)", "value": string(make([]byte, 1024*1024))})
	records = append(records, map[string]interface{}{"type": "10MB payload (io.Reader)", "value": string(make([]byte, 10*1024*1024))})
	records = append(records, map[string]interface{}{"type": "Slice with 1M elements", "value": make([]int, 1000000)})
	records = append(records, map[string]interface{}{"type": "Map with 100K keys", "value": make(map[string]int, 100000)})
	records = append(records, map[string]interface{}{"type": "Power-of-2 boundary strings", "value": string(make([]byte, 65536))})
	records = append(records, map[string]interface{}{"type": "Empty (0 byte) payload", "value": ""})

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
		return chaosdata.Payload{}, fmt.Errorf("volume: marshal payload: %w", err)
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
	chaosdata.Register(VolumeGenerator{})
}
