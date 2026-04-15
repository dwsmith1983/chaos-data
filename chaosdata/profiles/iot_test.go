package profiles

import (
	"bytes"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

func TestIoTGenerator_Name(t *testing.T) {
	g := IoTGenerator{}
	if g.Name() != "iot" {
		t.Errorf("expected iot, got %v", g.Name())
	}
}

func TestIoTGenerator_Category(t *testing.T) {
	g := IoTGenerator{}
	if g.Category() != "profiles" {
		t.Errorf("expected profiles, got %v", g.Category())
	}
}

func TestIoTGenerator_GenerateDefaultConfig(t *testing.T) {
	g := IoTGenerator{}
	payload, err := g.Generate(chaosdata.GenerateOpts{Count: 5})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if payload.Type != "application/json" {
		t.Errorf("expected json type, got %v", payload.Type)
	}
	if len(payload.Data) == 0 {
		t.Error("expected non-empty data")
	}
}

func TestIoTGenerator_DeviceFailure(t *testing.T) {
	g := IoTGenerator{}
	tags := map[string]string{"chaos_config": `{"error_rate": 1.0}`}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 5, Tags: tags})

	if !bytes.Contains(p.Data, []byte("offline")) {
		t.Error("expected offline status for failed device")
	}
}

func TestIoTGenerator_FirmwareCorruption(t *testing.T) {
	g := IoTGenerator{}
	tags := map[string]string{"chaos_config": `{"corruption_rate": 1.0}`}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 5, Tags: tags})

	if !bytes.Contains(p.Data, []byte("corrupted")) {
		t.Error("expected corrupted firmware with corruption_rate 1.0")
	}
}

func TestIoTGenerator_CSVOutput(t *testing.T) {
	g := IoTGenerator{}
	tags := map[string]string{"chaos_config": `{"output_format": "csv"}`}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 5, Tags: tags})

	if p.Type != "text/csv" {
		t.Errorf("expected text/csv type, got %v", p.Type)
	}
	if !bytes.HasPrefix(p.Data, []byte("device_id,timestamp,temperature,humidity,battery_level,firmware_version,gps_lat,gps_lon,status")) {
		t.Errorf("expected CSV headers, got %s", string(p.Data[:50]))
	}
}

func TestIoTGenerator_ExceedsMaxCount_ReturnsError(t *testing.T) {
	g := IoTGenerator{}
	_, err := g.Generate(chaosdata.GenerateOpts{Count: 200000})
	if err == nil {
		t.Error("expected error for exceeding MaxRecordCount")
	}
}
