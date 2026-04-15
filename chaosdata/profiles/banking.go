package profiles

import (
	"fmt"
	"time"

	"github.com/dwsmith1983/chaos-data/chaosdata"
)

// BankingGenerator generates chaos data for banking scenarios.
type BankingGenerator struct{}

// Name returns the name of the generator.
func (BankingGenerator) Name() string {
	return "banking"
}

// Category returns the category of the generator.
func (BankingGenerator) Category() string {
	return "profiles"
}

func init() {
	chaosdata.Register(BankingGenerator{})
}

var bankingColumns = []string{
	"tx_id", "account_from", "account_to", "amount", "currency", "timestamp", "status",
}

// Generate creates banking records.
func (g BankingGenerator) Generate(opts chaosdata.GenerateOpts) (chaosdata.Payload, error) {
	if opts.Count > MaxRecordCount {
		return chaosdata.Payload{}, fmt.Errorf("count %d exceeds max %d", opts.Count, MaxRecordCount)
	}

	cfg, err := ParseConfig(opts.Tags)
	if err != nil {
		return chaosdata.Payload{}, err
	}

	rng := NewSeededRand(cfg.Seed)

	count := opts.Count
	if count <= 0 {
		count = 1
	}

	var allRecords []map[string]interface{}
	baseTime := time.Now().UTC()

	// 1. Transaction stream base generation
	for i := 0; i < count; i++ {
		record := map[string]interface{}{
			"tx_id":        fmt.Sprintf("tx_%d", rng.Intn(1000000)),
			"account_from": fmt.Sprintf("acc_%d", rng.Intn(1000)),
			"account_to":   fmt.Sprintf("acc_%d", rng.Intn(1000)),
			"amount":       "100.00",
			"currency":     "USD",
			"timestamp":    baseTime.Add(time.Duration(i) * time.Minute).Format(time.RFC3339),
			"status":       "completed",
		}

		// Apply Multi-step rollback based on ErrorRate
		if rng.ShouldInject(cfg.ErrorRate) {
			record["status"] = rng.Pick([]string{"authorized", "captured", "settled", "rolled_back"})
		}

		// Apply Negative balance scenario based on ErrorRate
		if rng.ShouldInject(cfg.ErrorRate) {
			record["amount"] = "-50.00" // Overdraft
		}

		// Apply Currency edge cases
		if rng.ShouldInject(0.1) {
			currency := rng.Pick([]string{"JPY", "BHD", "ZWD"})
			record["currency"] = currency
			switch currency {
			case "JPY":
				record["amount"] = "100" // no decimals
			case "BHD":
				record["amount"] = "100.123" // 3 decimals
			case "ZWD":
				record["amount"] = "1000000000000.00" // hyperinflation
			}
		}

		// Apply Fraud injection based on ErrorRate
		if rng.ShouldInject(cfg.ErrorRate) {
			fraudType := rng.Pick([]string{"duplicate_tx", "impossible_velocity", "round_number", "card_testing"})
			switch fraudType {
			case "duplicate_tx":
				allRecords = append(allRecords, record) // Will be duplicated again
			case "impossible_velocity":
				// Handled by having same account multiple times quickly
				record["account_from"] = "acc_fraud_1"
			case "round_number":
				record["amount"] = "5000.00"
			case "card_testing":
				record["amount"] = "0.50"
			}
		}

		// Structural Chaos
		if rng.ShouldInject(cfg.SchemaDeviation) {
			delete(record, "status")
			record["extra_field"] = "unexpected"
		}

		// Semantic Chaos
		if rng.ShouldInject(cfg.CorruptionRate) {
			record["currency"] = "INVALID"
		}

		allRecords = append(allRecords, record)
	}

	// Volume Chaos (BurstSize)
	if cfg.BurstSize > 1 {
		var burstRecords []map[string]interface{}
		for _, r := range allRecords {
			for b := 0; b < cfg.BurstSize; b++ {
				cp := make(map[string]interface{}, len(r))
				for k, v := range r {
					cp[k] = v
				}
				burstRecords = append(burstRecords, cp)
			}
		}
		allRecords = burstRecords
	}

	// Temporal Chaos (OutOfOrderRate)
	if cfg.OutOfOrderRate > 0 {
		var shuffled []map[string]interface{}
		for _, idx := range rng.Shuffle(len(allRecords)) {
			// Also apply TimestampJitter
			rec := allRecords[idx]
			if tStr, ok := rec["timestamp"].(string); ok && cfg.TimestampJitter > 0 {
				if t, err := time.Parse(time.RFC3339, tStr); err == nil {
					rec["timestamp"] = rng.Jitter(t, cfg.TimestampJitter).Format(time.RFC3339)
				}
			}
			shuffled = append(shuffled, rec)
		}
		allRecords = shuffled
	}

	// Formatting
	data, err := Format(cfg.OutputFormat, allRecords, bankingColumns)
	if err != nil {
		return chaosdata.Payload{}, err
	}

	mimeType := "application/json"
	if cfg.OutputFormat == "csv" {
		mimeType = "text/csv"
	}

	return chaosdata.Payload{
		Data: data,
		Type: mimeType,
		Attributes: map[string]string{
			"generator": g.Name(),
			"category":  g.Category(),
			"records":   fmt.Sprintf("%d", len(allRecords)),
		},
	}, nil
}
