package aws_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"
)

// depsItem builds a DynamoDB item representing a downstream dependency record.
func depsItem(pipeline, downstream string) map[string]dtypes.AttributeValue {
	return map[string]dtypes.AttributeValue{
		"PK": &dtypes.AttributeValueMemberS{Value: chaosaws.DepsPK(pipeline)},
		"SK": &dtypes.AttributeValueMemberS{Value: chaosaws.DownstreamSK(downstream)},
	}
}

// queryRouter returns a QueryFn that dispatches based on the PK in the query.
func queryRouter(deps map[string][]string) func(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	return func(_ context.Context, input *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
		// Extract PK value from ExpressionAttributeValues :pk
		pkAttr, ok := input.ExpressionAttributeValues[":pk"]
		if !ok {
			return &dynamodb.QueryOutput{}, nil
		}
		pkVal, ok := pkAttr.(*dtypes.AttributeValueMemberS)
		if !ok {
			return &dynamodb.QueryOutput{}, nil
		}

		pipeline := strings.TrimPrefix(pkVal.Value, "DEPS#")

		downstreams, found := deps[pipeline]
		if !found {
			return &dynamodb.QueryOutput{Items: nil}, nil
		}

		items := make([]map[string]dtypes.AttributeValue, 0, len(downstreams))
		for _, ds := range downstreams {
			items = append(items, depsItem(pipeline, ds))
		}
		return &dynamodb.QueryOutput{Items: items}, nil
	}
}

func TestDynamoDBResolver_GetDownstream_DirectDeps(t *testing.T) {
	t.Parallel()

	mock := &mockDynamoDBAPI{
		QueryFn: queryRouter(map[string][]string{
			"bronze-cdr": {"silver-cdr-hour", "silver-cdr-day"},
		}),
	}

	resolver := chaosaws.NewDynamoDBDependencyResolver(mock, "interlock-control")
	got, err := resolver.GetDownstream(context.Background(), "bronze-cdr")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"silver-cdr-hour", "silver-cdr-day"}
	if len(got) != len(want) {
		t.Fatalf("got %d deps, want %d: %v", len(got), len(want), got)
	}
	for _, w := range want {
		found := false
		for _, g := range got {
			if g == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing expected downstream %q in %v", w, got)
		}
	}
}

func TestDynamoDBResolver_GetDownstream_Transitive(t *testing.T) {
	t.Parallel()

	mock := &mockDynamoDBAPI{
		QueryFn: queryRouter(map[string][]string{
			"ml-data-prep": {"ml-training"},
			"ml-training":  {"ml-evaluation"},
			"ml-evaluation": {"ml-deployment"},
		}),
	}

	resolver := chaosaws.NewDynamoDBDependencyResolver(mock, "interlock-control")
	got, err := resolver.GetDownstream(context.Background(), "ml-data-prep")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"ml-training", "ml-evaluation", "ml-deployment"}
	if len(got) != len(want) {
		t.Fatalf("got %d deps, want %d: %v", len(got), len(want), got)
	}
	for _, w := range want {
		found := false
		for _, g := range got {
			if g == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing expected downstream %q in %v", w, got)
		}
	}
}

func TestDynamoDBResolver_GetDownstream_NoDeps(t *testing.T) {
	t.Parallel()

	mock := &mockDynamoDBAPI{
		QueryFn: queryRouter(map[string][]string{}),
	}

	resolver := chaosaws.NewDynamoDBDependencyResolver(mock, "interlock-control")
	got, err := resolver.GetDownstream(context.Background(), "iot-factory")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestDynamoDBResolver_GetDownstream_CycleProtection(t *testing.T) {
	t.Parallel()

	mock := &mockDynamoDBAPI{
		QueryFn: queryRouter(map[string][]string{
			"A": {"B"},
			"B": {"A"},
		}),
	}

	resolver := chaosaws.NewDynamoDBDependencyResolver(mock, "interlock-control")
	got, err := resolver.GetDownstream(context.Background(), "A")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should find B (direct dep of A), and A is excluded (original target).
	// B→A is a cycle back to origin, so only B should appear.
	if len(got) != 1 {
		t.Fatalf("expected 1 dep, got %d: %v", len(got), got)
	}
	if got[0] != "B" {
		t.Errorf("expected [B], got %v", got)
	}
}

func TestDynamoDBResolver_GetDownstream_ErrorSurfaced(t *testing.T) {
	t.Parallel()

	mock := &mockDynamoDBAPI{
		QueryFn: func(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return nil, fmt.Errorf("simulated DynamoDB error")
		},
	}

	resolver := chaosaws.NewDynamoDBDependencyResolver(mock, "interlock-control")
	got, err := resolver.GetDownstream(context.Background(), "any-pipeline")
	if err == nil {
		t.Fatal("expected error to be surfaced, got nil")
	}
	if got != nil {
		t.Errorf("expected nil result on first-query error, got %v", got)
	}
}
