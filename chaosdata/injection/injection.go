package injection

import (
	"encoding/json"
	"fmt"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

type InjectionGenerator struct{}

func (InjectionGenerator) Name() string {
	return "injection"
}

func (InjectionGenerator) Category() string {
	return "injection"
}

func (g InjectionGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	records := []map[string]interface{}{}
	records = append(records, map[string]interface{}{"type": "SQL injection (' OR 1=1 --)", "value": "' OR 1=1 --"})
	records = append(records, map[string]interface{}{"type": "SQL injection (UNION SELECT)", "value": "UNION SELECT username, password FROM users"})
	records = append(records, map[string]interface{}{"type": "XSS (<script>)", "value": "<script>alert('xss')</script>"})
	records = append(records, map[string]interface{}{"type": "XSS (event handlers)", "value": "<img src=x onerror=alert(1)>"})
	records = append(records, map[string]interface{}{"type": "Command injection (; rm -rf /)", "value": "; rm -rf /"})
	records = append(records, map[string]interface{}{"type": "Command injection (backticks)", "value": "`id`"})
	records = append(records, map[string]interface{}{"type": "Command injection ($())", "value": "$(whoami)"})
	records = append(records, map[string]interface{}{"type": "LDAP injection", "value": "*)(uid=*))(|(uid=*"})
	records = append(records, map[string]interface{}{"type": "Header injection (CRLF)", "value": "admin\r\nSet-Cookie: session=123"})
	records = append(records, map[string]interface{}{"type": "Go template injection ({{.}})", "value": "{{.}}"})

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
		return chaosdata.Payload{}, fmt.Errorf("injection: marshal payload: %w", err)
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
	chaosdata.Register(InjectionGenerator{})
}
