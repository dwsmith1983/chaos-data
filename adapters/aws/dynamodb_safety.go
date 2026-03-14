package aws

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface assertion.
var _ adapter.SafetyController = (*DynamoDBSafety)(nil)

// slaWindowDuration is the duration before an SLA deadline during which
// chaos injection is considered unsafe.
const slaWindowDuration = 30 * time.Minute

// DynamoDBSafety implements adapter.SafetyController using a DynamoDB
// table for control records. All operations fail-safe: missing records
// or errors result in the most conservative (safest) return value.
type DynamoDBSafety struct {
	api       DynamoDBAPI
	tableName string
}

// NewDynamoDBSafety creates a DynamoDBSafety backed by the given
// DynamoDBAPI client and table name.
func NewDynamoDBSafety(api DynamoDBAPI, tableName string) *DynamoDBSafety {
	return &DynamoDBSafety{
		api:       api,
		tableName: tableName,
	}
}

// IsEnabled checks whether chaos injection is globally enabled.
// The control record uses PK=SK="CONTROL#chaos-enabled" with a "value"
// attribute of "true" or "false".
//
// Fail-safe: missing record or error returns false (disabled).
func (s *DynamoDBSafety) IsEnabled(ctx context.Context) (bool, error) {
	item, err := s.getControl(ctx, "chaos-enabled", "chaos-enabled")
	if err != nil {
		return false, err
	}
	if item == nil {
		return false, nil
	}

	val, ok := stringAttr(item, "value")
	if !ok {
		return false, nil
	}
	return val == "true", nil
}

// MaxSeverity returns the maximum allowed chaos severity level.
// The control record uses PK=SK="CONTROL#max-severity" with a "value"
// attribute containing a severity name (e.g. "moderate").
//
// Fail-safe: missing record or error returns SeverityLow.
func (s *DynamoDBSafety) MaxSeverity(ctx context.Context) (types.Severity, error) {
	item, err := s.getControl(ctx, "max-severity", "max-severity")
	if err != nil {
		return types.SeverityLow, err
	}
	if item == nil {
		return types.SeverityLow, nil
	}

	val, ok := stringAttr(item, "value")
	if !ok {
		return types.SeverityLow, nil
	}

	sev, err := types.ParseSeverity(val)
	if err != nil {
		// Fail-safe: corrupt/invalid config value defaults to SeverityLow.
		return types.SeverityLow, nil
	}
	return sev, nil
}

// CheckBlastRadius verifies that experiment stats are within the
// configured blast-radius limits. The control record uses
// PK=SK="CONTROL#blast-radius" with "max_affected_pct" (number) and
// "max_pipelines" (number) attributes.
//
// Fail-safe: missing record means no limits are enforced (returns nil).
func (s *DynamoDBSafety) CheckBlastRadius(ctx context.Context, stats types.ExperimentStats) error {
	item, err := s.getControl(ctx, "blast-radius", "blast-radius")
	if err != nil {
		return err
	}
	if item == nil {
		return nil
	}

	if maxPctStr, ok := numberAttr(item, "max_affected_pct"); ok {
		maxPct, err := strconv.ParseFloat(maxPctStr, 64)
		if err != nil {
			return fmt.Errorf("dynamodb safety: parse max_affected_pct: %w", err)
		}
		if stats.AffectedPct > maxPct {
			return fmt.Errorf(
				"blast radius exceeded: affected %.1f%% > max %.0f%%",
				stats.AffectedPct, maxPct,
			)
		}
	}

	if maxPipelinesStr, ok := numberAttr(item, "max_pipelines"); ok {
		maxPipelines, err := strconv.Atoi(maxPipelinesStr)
		if err != nil {
			return fmt.Errorf("dynamodb safety: parse max_pipelines: %w", err)
		}
		if stats.AffectedPipelines > maxPipelines {
			return fmt.Errorf(
				"blast radius exceeded: %d pipelines > max %d",
				stats.AffectedPipelines, maxPipelines,
			)
		}
	}

	return nil
}

// CheckSLAWindow reports whether chaos injection is safe for the given
// pipeline. It returns true when the pipeline is NOT within an SLA
// window and chaos injection may proceed. It returns false when the
// pipeline IS within 30 minutes of its SLA deadline.
//
// The control record uses PK="CONTROL#sla-window", SK=pipeline with a
// "deadline" attribute in RFC3339 format.
//
// Fail-safe: missing record returns true (safe to proceed).
func (s *DynamoDBSafety) CheckSLAWindow(ctx context.Context, pipeline string) (bool, error) {
	item, err := s.getControl(ctx, "sla-window", pipeline)
	if err != nil {
		return true, err
	}
	if item == nil {
		return true, nil
	}

	deadlineStr, ok := stringAttr(item, "deadline")
	if !ok {
		return true, nil
	}

	deadline, err := time.Parse(time.RFC3339, deadlineStr)
	if err != nil {
		return true, fmt.Errorf("dynamodb safety: parse deadline: %w", err)
	}

	remaining := time.Until(deadline)
	if remaining > 0 && remaining <= slaWindowDuration {
		return false, nil
	}
	return true, nil
}

// getControl fetches a control record from DynamoDB.
// It returns nil, nil when the item does not exist.
func (s *DynamoDBSafety) getControl(ctx context.Context, pkName, skName string) (map[string]dtypes.AttributeValue, error) {
	pk := ControlPK(pkName)
	sk := skName
	// When PK and SK names are the same control key, use the full
	// CONTROL# prefix for both. Otherwise SK is a plain value
	// (e.g. pipeline name).
	if pkName == skName {
		sk = pk
	}

	out, err := s.api.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.tableName,
		Key: map[string]dtypes.AttributeValue{
			"PK": &dtypes.AttributeValueMemberS{Value: pk},
			"SK": &dtypes.AttributeValueMemberS{Value: sk},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb safety: get %s: %w", pkName, err)
	}
	if len(out.Item) == 0 {
		return nil, nil
	}
	return out.Item, nil
}

// stringAttr extracts a string attribute value from a DynamoDB item.
func stringAttr(item map[string]dtypes.AttributeValue, key string) (string, bool) {
	av, ok := item[key]
	if !ok {
		return "", false
	}
	s, ok := av.(*dtypes.AttributeValueMemberS)
	if !ok {
		return "", false
	}
	return s.Value, true
}

// numberAttr extracts a number attribute value (as string) from a DynamoDB item.
func numberAttr(item map[string]dtypes.AttributeValue, key string) (string, bool) {
	av, ok := item[key]
	if !ok {
		return "", false
	}
	n, ok := av.(*dtypes.AttributeValueMemberN)
	if !ok {
		return "", false
	}
	return n.Value, true
}
