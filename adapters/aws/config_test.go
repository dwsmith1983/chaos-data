package aws_test

import (
	"testing"

	awsadapter "github.com/dwsmith1983/chaos-data/adapters/aws"
)

func TestConfig_Validate_MissingStagingBucket(t *testing.T) {
	t.Parallel()

	cfg := awsadapter.Config{
		PipelineBucket: "pipeline-bucket",
		TableName:      "chaos-table",
	}
	cfg.Defaults()

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() = nil, want error for missing StagingBucket")
	}
}

func TestConfig_Validate_MissingPipelineBucket(t *testing.T) {
	t.Parallel()

	cfg := awsadapter.Config{
		StagingBucket: "staging-bucket",
		TableName:     "chaos-table",
	}
	cfg.Defaults()

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() = nil, want error for missing PipelineBucket")
	}
}

func TestConfig_Validate_MissingTableName(t *testing.T) {
	t.Parallel()

	cfg := awsadapter.Config{
		StagingBucket:  "staging-bucket",
		PipelineBucket: "pipeline-bucket",
	}
	cfg.Defaults()

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() = nil, want error for missing TableName")
	}
}

func TestConfig_Validate_Happy(t *testing.T) {
	t.Parallel()

	cfg := awsadapter.Config{
		StagingBucket:  "staging-bucket",
		PipelineBucket: "pipeline-bucket",
		TableName:      "chaos-table",
	}
	cfg.Defaults()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestConfig_Defaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     awsadapter.Config
		wantField string
		wantValue string
	}{
		{
			name:      "region defaults to us-east-1",
			input:     awsadapter.Config{},
			wantField: "Region",
			wantValue: "us-east-1",
		},
		{
			name:      "event bus defaults to default",
			input:     awsadapter.Config{},
			wantField: "EventBusName",
			wantValue: "default",
		},
		{
			name:      "hold prefix defaults to chaos-hold/",
			input:     awsadapter.Config{},
			wantField: "HoldPrefix",
			wantValue: "chaos-hold/",
		},
		{
			name:      "region not overwritten when set",
			input:     awsadapter.Config{Region: "eu-west-1"},
			wantField: "Region",
			wantValue: "eu-west-1",
		},
		{
			name:      "event bus not overwritten when set",
			input:     awsadapter.Config{EventBusName: "custom-bus"},
			wantField: "EventBusName",
			wantValue: "custom-bus",
		},
		{
			name:      "hold prefix not overwritten when set",
			input:     awsadapter.Config{HoldPrefix: "custom-hold/"},
			wantField: "HoldPrefix",
			wantValue: "custom-hold/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := tt.input
			cfg.Defaults()

			var got string
			switch tt.wantField {
			case "Region":
				got = cfg.Region
			case "EventBusName":
				got = cfg.EventBusName
			case "HoldPrefix":
				got = cfg.HoldPrefix
			default:
				t.Fatalf("unknown field %q", tt.wantField)
			}

			if got != tt.wantValue {
				t.Errorf("%s = %q, want %q", tt.wantField, got, tt.wantValue)
			}
		})
	}
}
