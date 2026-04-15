package profiles

import (
	"bytes"
	"testing"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

func TestBankingGenerator_Name(t *testing.T) {
	g := BankingGenerator{}
	if g.Name() != "banking" {
		t.Errorf("expected banking, got %v", g.Name())
	}
}

func TestBankingGenerator_Category(t *testing.T) {
	g := BankingGenerator{}
	if g.Category() != "profiles" {
		t.Errorf("expected profiles, got %v", g.Category())
	}
}

func TestBankingGenerator_GenerateDefaultConfig(t *testing.T) {
	g := BankingGenerator{}
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

func TestBankingGenerator_DeterministicReplay(t *testing.T) {
	g := BankingGenerator{}
	tags := map[string]string{"chaos_config": `{"seed": 42}`}
	
	p1, _ := g.Generate(chaosdata.GenerateOpts{Count: 10, Tags: tags})
	p2, _ := g.Generate(chaosdata.GenerateOpts{Count: 10, Tags: tags})

	if !bytes.Equal(p1.Data, p2.Data) {
		t.Error("expected deterministic output for same seed")
	}

	tagsDiff := map[string]string{"chaos_config": `{"seed": 43}`}
	p3, _ := g.Generate(chaosdata.GenerateOpts{Count: 10, Tags: tagsDiff})

	if bytes.Equal(p1.Data, p3.Data) {
		t.Error("expected different output for different seed")
	}
}

func TestBankingGenerator_NegativeBalance(t *testing.T) {
	g := BankingGenerator{}
	tags := map[string]string{"chaos_config": `{"error_rate": 1.0}`}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 5, Tags: tags})

	if !bytes.Contains(p.Data, []byte("-50.00")) {
		t.Error("expected negative balance with error_rate 1.0")
	}
}

func TestBankingGenerator_FraudInjection(t *testing.T) {
	g := BankingGenerator{}
	tags := map[string]string{"chaos_config": `{"error_rate": 1.0}`}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 20, Tags: tags})

	// Fraud patterns will cause account_from to be acc_fraud_1, amount to be 5000.00 or 0.50
	if !bytes.Contains(p.Data, []byte("acc_fraud_1")) && 
	   !bytes.Contains(p.Data, []byte("5000.00")) && 
	   !bytes.Contains(p.Data, []byte("0.50")) {
		t.Error("expected fraud patterns with error_rate 1.0")
	}
}

func TestBankingGenerator_MultiStepRollback(t *testing.T) {
	g := BankingGenerator{}
	tags := map[string]string{"chaos_config": `{"error_rate": 1.0}`}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 20, Tags: tags})

	// Since error_rate is 1.0, all records will have a random status
	if !bytes.Contains(p.Data, []byte("authorized")) &&
	   !bytes.Contains(p.Data, []byte("captured")) &&
	   !bytes.Contains(p.Data, []byte("settled")) &&
	   !bytes.Contains(p.Data, []byte("rolled_back")) {
		t.Error("expected multi-step rollback status with error_rate 1.0")
	}
}

func TestBankingGenerator_CurrencyEdgeCases(t *testing.T) {
	g := BankingGenerator{}
	// Set error_rate to 0 to avoid overriding amount, rely on currency logic
	tags := map[string]string{"chaos_config": `{"seed": 42}`}
	// Run enough records to ensure 10% currency edge case hits
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 100, Tags: tags})

	if !bytes.Contains(p.Data, []byte("JPY")) &&
	   !bytes.Contains(p.Data, []byte("BHD")) &&
	   !bytes.Contains(p.Data, []byte("ZWD")) {
		t.Error("expected currency edge cases")
	}
}

func TestBankingGenerator_CSVOutput(t *testing.T) {
	g := BankingGenerator{}
	tags := map[string]string{"chaos_config": `{"output_format": "csv"}`}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 5, Tags: tags})

	if p.Type != "text/csv" {
		t.Errorf("expected text/csv type, got %v", p.Type)
	}
	if !bytes.HasPrefix(p.Data, []byte("tx_id,account_from,account_to,amount,currency,timestamp,status")) {
		t.Errorf("expected CSV headers, got %s", string(p.Data[:50]))
	}
}

func TestBankingGenerator_ExceedsMaxCount_ReturnsError(t *testing.T) {
	g := BankingGenerator{}
	_, err := g.Generate(chaosdata.GenerateOpts{Count: 200000})
	if err == nil {
		t.Error("expected error for exceeding MaxRecordCount")
	}
}

func TestBankingGenerator_ChaosDensity(t *testing.T) {
	g := BankingGenerator{}
	tags := map[string]string{"chaos_config": `{"error_rate": 1.0, "corruption_rate": 1.0}`}
	p, _ := g.Generate(chaosdata.GenerateOpts{Count: 5, Tags: tags})

	// All should have corrupted currency
	if bytes.Contains(p.Data, []byte("USD")) {
		t.Error("expected 100% corruption rate to eliminate USD")
	}
	if !bytes.Contains(p.Data, []byte("INVALID")) {
		t.Error("expected INVALID currency from corruption")
	}
}
