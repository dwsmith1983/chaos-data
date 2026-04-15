package profiles

import (
	"bytes"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

func TestTelecomGenerator_Name(t *testing.T) {
	g := TelecomGenerator{}
	if g.Name() != "telecom" {
		t.Errorf("expected telecom, got %v", g.Name())
	}
}

func TestTelecomGenerator_Category(t *testing.T) {
	g := TelecomGenerator{}
	if g.Category() != "profiles" {
		t.Errorf("expected profiles, got %v", g.Category())
	}
}

func TestTelecomGenerator_GenerateDefaultConfig(t *testing.T) {
	g := TelecomGenerator{}
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

func TestTelecomGenerator_DroppedCalls(t *testing.T) {
	g := TelecomGenerator{}
	tags := map[string]string{"chaos_config": `{"error_rate": 1.0}`}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 5, Tags: tags})

	if !bytes.Contains(p.Data, []byte("dropped")) {
		t.Error("expected dropped status with error_rate 1.0")
	}
}

func TestTelecomGenerator_RoamingHandoffs(t *testing.T) {
	g := TelecomGenerator{}
	tags := map[string]string{"chaos_config": `{"error_rate": 1.0}`}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 5, Tags: tags})

	if !bytes.Contains(p.Data, []byte("_roaming")) {
		t.Error("expected roaming handoff cell towers with error_rate 1.0")
	}
}

func TestTelecomGenerator_SemanticChaos(t *testing.T) {
	g := TelecomGenerator{}
	tags := map[string]string{"chaos_config": `{"corruption_rate": 1.0}`}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 5, Tags: tags})

	if !bytes.Contains(p.Data, []byte("-10")) {
		t.Error("expected impossible negative duration with corruption_rate 1.0")
	}
}

func TestTelecomGenerator_CSVOutput(t *testing.T) {
	g := TelecomGenerator{}
	tags := map[string]string{"chaos_config": `{"output_format": "csv"}`}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 5, Tags: tags})

	if p.Type != "text/csv" {
		t.Errorf("expected text/csv type, got %v", p.Type)
	}
	if !bytes.HasPrefix(p.Data, []byte("call_id,caller_id,callee_id,caller_imsi,callee_imsi,cell_tower_id,call_type,start_time,end_time,duration_sec,bytes_transferred,status")) {
		t.Errorf("expected CSV headers, got %s", string(p.Data[:50]))
	}
}

func TestTelecomGenerator_ExceedsMaxCount_ReturnsError(t *testing.T) {
	g := TelecomGenerator{}
	_, err := g.Generate(chaosdata.GenerateOpts{Count: 200000})
	if err == nil {
		t.Error("expected error for exceeding MaxRecordCount")
	}
}
