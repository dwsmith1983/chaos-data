package config

import (
	"fmt"
	"os"

	"github.com/dwsmith1983/chaos-data/adapters/airflow"
	"github.com/dwsmith1983/chaos-data/adapters/dagster"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"gopkg.in/yaml.v3"
)

// Config is the top-level chaos-data configuration file structure.
type Config struct {
	Adapters AdapterConfig `yaml:"adapters"`
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
	URL     string            `yaml:"url"`
	Headers map[string]string `yaml:"headers"`
}

// Load reads and parses a YAML config file.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: read %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("config: parse %s: %w", path, err)
	}
	return cfg, nil
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
			URL:     c.Adapters.Airflow.URL,
			Headers: c.Adapters.Airflow.Headers,
		}
		cfg.Defaults()
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("config: airflow: %w", err)
		}
		client := airflow.NewRESTClient(cfg)
		return airflow.NewAirflowAsserter(client), nil
	}

	return nil, nil
}
