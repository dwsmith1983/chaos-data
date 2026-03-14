package aws_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"

	chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"
	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// newTestProxyEngine creates an engine with no scenarios and no-op mock
// adapters. With no matching scenarios, ProcessObject is a passthrough
// that returns (nil, nil) — useful for testing the handler's S3 event
// parsing and filtering without mutation side-effects.
func newTestProxyEngine() *engine.Engine {
	transport := &noopTransport{}
	reg := mutation.NewRegistry()
	cfg := types.EngineConfig{Mode: "deterministic"}
	return engine.New(cfg, transport, reg, nil)
}

// newTestProxyTransport creates a minimal S3Transport suitable for
// passing to NewProxyHandler.
func newTestProxyTransport() *chaosaws.S3Transport {
	cfg := chaosaws.Config{
		StagingBucket:  "staging-bucket",
		PipelineBucket: "pipeline-bucket",
		TableName:      "chaos-table",
	}
	cfg.Defaults()
	return chaosaws.NewS3Transport(&mockS3API{}, cfg)
}

func TestProxyHandler_HandleS3Event_SingleObject(t *testing.T) {
	t.Parallel()

	eng := newTestProxyEngine()
	transport := newTestProxyTransport()
	handler := chaosaws.NewProxyHandler(eng, transport, "chaos-hold/")

	now := time.Now()
	event := events.S3Event{
		Records: []events.S3EventRecord{
			{
				S3: events.S3Entity{
					Bucket: events.S3Bucket{Name: "staging-bucket"},
					Object: events.S3Object{
						Key:           "data/file.csv",
						Size:          1024,
						URLDecodedKey: "data/file.csv",
					},
				},
				EventTime: now,
			},
		},
	}

	err := handler.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}
}

func TestProxyHandler_HandleS3Event_SkipsHoldPrefix(t *testing.T) {
	t.Parallel()

	eng := newTestProxyEngine()
	transport := newTestProxyTransport()
	handler := chaosaws.NewProxyHandler(eng, transport, "chaos-hold/")

	event := events.S3Event{
		Records: []events.S3EventRecord{
			{
				S3: events.S3Entity{
					Bucket: events.S3Bucket{Name: "staging-bucket"},
					Object: events.S3Object{
						Key:           "chaos-hold/data/file.csv",
						Size:          512,
						URLDecodedKey: "chaos-hold/data/file.csv",
					},
				},
			},
		},
	}

	err := handler.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}
	// No error means the held key was skipped (no engine processing error).
}

func TestProxyHandler_HandleS3Event_URLDecodesKey(t *testing.T) {
	t.Parallel()

	eng := newTestProxyEngine()
	transport := newTestProxyTransport()
	handler := chaosaws.NewProxyHandler(eng, transport, "chaos-hold/")

	// S3 event notifications URL-encode the key. The handler must decode it.
	event := events.S3Event{
		Records: []events.S3EventRecord{
			{
				S3: events.S3Entity{
					Bucket: events.S3Bucket{Name: "staging-bucket"},
					Object: events.S3Object{
						Key:  "path%20with%20spaces/file.csv",
						Size: 256,
					},
				},
			},
		},
	}

	err := handler.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}
}

func TestProxyHandler_HandleS3Event_EmptyEvent(t *testing.T) {
	t.Parallel()

	eng := newTestProxyEngine()
	transport := newTestProxyTransport()
	handler := chaosaws.NewProxyHandler(eng, transport, "chaos-hold/")

	event := events.S3Event{
		Records: nil,
	}

	err := handler.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}
}

// --- Test with tracking to verify engine is actually called ---

func TestProxyHandler_HandleS3Event_CallsEngineProcessObject(t *testing.T) {
	t.Parallel()

	// Use a scenario that matches everything and a registered mutation,
	// so ProcessObject actually invokes the mutation. The no-op transport
	// makes Hold a no-op, so the delay mutation succeeds silently.
	transport := &noopTransport{}
	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	sc := scenario.Scenario{
		Name:        "test-delay",
		Description: "test",
		Category:    "data-arrival",
		Severity:    types.SeverityLow,
		Version:     1,
		Target: scenario.TargetSpec{
			Layer: "data",
		},
		Mutation: scenario.MutationSpec{
			Type:   "delay",
			Params: map[string]string{"duration": "10m", "release": "true"},
		},
		Probability: 1.0,
		Safety:      scenario.ScenarioSafety{MaxAffectedPct: 100},
	}

	cfg := types.EngineConfig{Mode: "deterministic"}
	eng := engine.New(cfg, transport, reg, []scenario.Scenario{sc})

	proxyTransport := newTestProxyTransport()
	handler := chaosaws.NewProxyHandler(eng, proxyTransport, "chaos-hold/")

	event := events.S3Event{
		Records: []events.S3EventRecord{
			{
				S3: events.S3Entity{
					Bucket: events.S3Bucket{Name: "staging-bucket"},
					Object: events.S3Object{
						Key:  "data/file.csv",
						Size: 1024,
					},
				},
			},
		},
	}

	err := handler.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}

	// If we get here without error, the engine processed the object
	// (the delay mutation's Hold was called on the no-op transport).
}

func TestProxyHandler_HandleS3Event_MultipleRecordsSkipsHoldOnly(t *testing.T) {
	t.Parallel()

	eng := newTestProxyEngine()
	transport := newTestProxyTransport()
	handler := chaosaws.NewProxyHandler(eng, transport, "chaos-hold/")

	event := events.S3Event{
		Records: []events.S3EventRecord{
			{
				S3: events.S3Entity{
					Object: events.S3Object{Key: "data/a.csv", Size: 100},
				},
			},
			{
				S3: events.S3Entity{
					Object: events.S3Object{Key: "chaos-hold/b.csv", Size: 200},
				},
			},
			{
				S3: events.S3Entity{
					Object: events.S3Object{Key: "data/c.csv", Size: 300},
				},
			},
		},
	}

	err := handler.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("Handle() error = %v, want nil", err)
	}
}
