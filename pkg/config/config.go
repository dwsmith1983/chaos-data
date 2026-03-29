package config

import (
	"context"
	"fmt"
	"io"
	"os"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dwsmith1983/chaos-data/adapters/airflow"
	chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"
	"github.com/dwsmith1983/chaos-data/adapters/dagster"
	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
	"gopkg.in/yaml.v3"
)

// Config is the top-level chaos-data configuration file structure.
type Config struct {
	Transport TransportConfig `yaml:"transport"`
	Safety    SafetyConfig    `yaml:"safety"`
	Emitter   EmitterConfig   `yaml:"emitter"`
	Adapters  AdapterConfig   `yaml:"adapters"`
}

// TransportConfig specifies how data is moved between staging and pipeline.
type TransportConfig struct {
	Type           string `yaml:"type"`
	Region         string `yaml:"region"`
	StagingBucket  string `yaml:"staging_bucket"`
	PipelineBucket string `yaml:"pipeline_bucket"`
	HoldPrefix     string `yaml:"hold_prefix"`
	InputDir       string `yaml:"input_dir"`
	OutputDir      string `yaml:"output_dir"`
}

// SafetyConfig specifies the safety controller backend.
type SafetyConfig struct {
	Type      string `yaml:"type"`
	Region    string `yaml:"region"`
	TableName string `yaml:"table_name"`
}

// EmitterConfig specifies the event emitter backend.
type EmitterConfig struct {
	Type         string `yaml:"type"`
	Region       string `yaml:"region"`
	EventBusName string `yaml:"event_bus_name"`
}

// AdapterConfig holds configuration for external orchestrator adapters.
type AdapterConfig struct {
	Dagster DagsterAdapterConfig `yaml:"dagster"`
	Airflow AirflowAdapterConfig `yaml:"airflow"`
}

// DagsterAdapterConfig maps 1:1 to dagster.Config with YAML tags.
type DagsterAdapterConfig struct {
	URL                    string            `yaml:"url"`
	Headers                map[string]string `yaml:"headers"`
	RepositoryLocationName string            `yaml:"repository_location_name"`
	RepositoryName         string            `yaml:"repository_name"`
}

// AirflowAdapterConfig maps 1:1 to airflow.Config with YAML tags.
type AirflowAdapterConfig struct {
	URL      string            `yaml:"url"`
	Headers  map[string]string `yaml:"headers"`
	Version  string            `yaml:"version"`
	Username string            `yaml:"username"`
	Password string            `yaml:"password"`
}

// maxConfigSize is the maximum allowed size for config data (1 MB).
const maxConfigSize = 1 << 20

// LoadFromBytes parses raw YAML bytes into a Config.
func LoadFromBytes(data []byte) (Config, error) {
	if len(data) > maxConfigSize {
		return Config{}, fmt.Errorf("config: data size %d exceeds maximum %d bytes", len(data), maxConfigSize)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("config: parse: %w", err)
	}
	return cfg, nil
}

// Load reads and parses a YAML config file.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: read %s: %w", path, err)
	}
	if len(data) > maxConfigSize {
		return Config{}, fmt.Errorf("config: file %s size %d exceeds maximum %d bytes", path, len(data), maxConfigSize)
	}
	return LoadFromBytes(data)
}

// Validate checks that at most one adapter is configured.
func (c Config) Validate() error {
	if c.Adapters.Dagster.URL != "" && c.Adapters.Airflow.URL != "" {
		return fmt.Errorf("config: at most one adapter may be configured (both dagster and airflow have URLs)")
	}
	return nil
}

// BuildAsserter constructs the appropriate Asserter from the configured
// adapter. Returns (nil, nil) if no adapter is configured.
func (c Config) BuildAsserter() (adapter.Asserter, error) {
	if c.Adapters.Dagster.URL != "" {
		cfg := dagster.Config{
			URL:                    c.Adapters.Dagster.URL,
			Headers:                c.Adapters.Dagster.Headers,
			RepositoryLocationName: c.Adapters.Dagster.RepositoryLocationName,
			RepositoryName:         c.Adapters.Dagster.RepositoryName,
		}
		cfg.Defaults()
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("config: dagster: %w", err)
		}
		client := dagster.NewGraphQLClient(cfg)
		return dagster.NewDagsterAsserter(client), nil
	}

	if c.Adapters.Airflow.URL != "" {
		cfg := airflow.Config{
			URL:      c.Adapters.Airflow.URL,
			Headers:  c.Adapters.Airflow.Headers,
			Version:  c.Adapters.Airflow.Version,
			Username: c.Adapters.Airflow.Username,
			Password: c.Adapters.Airflow.Password,
		}
		cfg.Defaults()
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("config: airflow: %w", err)
		}
		var client airflow.AirflowAPI
		if cfg.Version == "v2" {
			client = airflow.NewRESTClientV2(cfg)
		} else {
			client = airflow.NewRESTClient(cfg)
		}
		return airflow.NewAirflowAsserter(client), nil
	}

	return nil, nil
}

// BuildTransport constructs a DataTransport from the transport config section.
// Returns (nil, nil) when no transport is configured.
func (c Config) BuildTransport(ctx context.Context) (adapter.DataTransport, error) {
	switch c.Transport.Type {
	case "s3":
		if c.Transport.StagingBucket == "" {
			return nil, fmt.Errorf("config: transport s3: staging_bucket is required")
		}
		if c.Transport.PipelineBucket == "" {
			return nil, fmt.Errorf("config: transport s3: pipeline_bucket is required")
		}
		opts := []func(*awsconfig.LoadOptions) error{}
		if c.Transport.Region != "" {
			opts = append(opts, awsconfig.WithRegion(c.Transport.Region))
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("config: transport s3: load aws config: %w", err)
		}
		cfg := chaosaws.Config{
			StagingBucket:  c.Transport.StagingBucket,
			PipelineBucket: c.Transport.PipelineBucket,
			HoldPrefix:     c.Transport.HoldPrefix,
		}
		cfg.Defaults()
		s3Client := s3.NewFromConfig(awsCfg)
		return chaosaws.NewS3Transport(s3Client, cfg), nil
	case "fs":
		return local.NewFSTransport(c.Transport.InputDir, c.Transport.OutputDir), nil
	case "":
		return nil, nil
	default:
		return nil, fmt.Errorf("config: unknown transport type %q", c.Transport.Type)
	}
}

// BuildSafety constructs a SafetyController from the safety config section.
// Defaults to a ConfigSafety when no type is specified.
func (c Config) BuildSafety(ctx context.Context) (adapter.SafetyController, error) {
	switch c.Safety.Type {
	case "dynamodb":
		if c.Safety.TableName == "" {
			return nil, fmt.Errorf("config: safety dynamodb: table_name is required")
		}
		opts := []func(*awsconfig.LoadOptions) error{}
		if c.Safety.Region != "" {
			opts = append(opts, awsconfig.WithRegion(c.Safety.Region))
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("config: safety dynamodb: load aws config: %w", err)
		}
		dynamoClient := dynamodb.NewFromConfig(awsCfg)
		return chaosaws.NewDynamoDBSafety(
			dynamoClient,
			c.Safety.TableName,
			types.Defaults().Safety.CooldownDuration.Duration,
		), nil
	case "", "config":
		return local.NewConfigSafety(types.Defaults().Safety), nil
	case "none":
		return nil, nil
	default:
		return nil, fmt.Errorf("config: unknown safety type %q", c.Safety.Type)
	}
}

// BuildEmitter constructs an EventEmitter from the emitter config section.
// Defaults to a StdoutEmitter when no type is specified.
func (c Config) BuildEmitter(ctx context.Context, w io.Writer) (adapter.EventEmitter, error) {
	switch c.Emitter.Type {
	case "eventbridge":
		opts := []func(*awsconfig.LoadOptions) error{}
		if c.Emitter.Region != "" {
			opts = append(opts, awsconfig.WithRegion(c.Emitter.Region))
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("config: emitter eventbridge: load aws config: %w", err)
		}
		busName := c.Emitter.EventBusName
		if busName == "" {
			busName = "default"
		}
		ebClient := eventbridge.NewFromConfig(awsCfg)
		return chaosaws.NewEventBridgeEmitter(ebClient, busName), nil
	case "", "stdout":
		return local.NewStdoutEmitter(w), nil
	case "none":
		return nil, nil
	default:
		return nil, fmt.Errorf("config: unknown emitter type %q", c.Emitter.Type)
	}
}
