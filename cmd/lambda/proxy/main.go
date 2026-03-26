// Package main provides the AWS Lambda entry point for S3-triggered chaos
// injection. It reads configuration from environment variables, assembles
// the adapter stack, and starts the Lambda runtime.
package main

import (
	"context"
	"log"
	"os"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/aws-lambda-go/lambda"

	chaosaws "github.com/dwsmith1983/chaos-data/adapters/aws"
	"github.com/dwsmith1983/chaos-data/pkg/config"
	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

func main() {
	ctx := context.Background()

	stagingBucket := requireEnv("STAGING_BUCKET")
	pipelineBucket := requireEnv("PIPELINE_BUCKET")
	tableName := requireEnv("TABLE_NAME")
	eventBusName := envOrDefault("EVENT_BUS_NAME", "default")

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}

	cfg := chaosaws.Config{
		StagingBucket:  stagingBucket,
		PipelineBucket: pipelineBucket,
		TableName:      tableName,
		EventBusName:   eventBusName,
	}
	cfg.Defaults()

	s3Client := s3.NewFromConfig(awsCfg)
	dynamoClient := dynamodb.NewFromConfig(awsCfg)
	ebClient := eventbridge.NewFromConfig(awsCfg)

	transport := chaosaws.NewS3Transport(s3Client, cfg)
	state := chaosaws.NewDynamoDBState(dynamoClient, tableName)
	emitter := chaosaws.NewEventBridgeEmitter(ebClient, eventBusName)
	safety := chaosaws.NewDynamoDBSafety(dynamoClient, tableName, types.Defaults().Safety.CooldownDuration.Duration)

	// Suppress unused variable warnings for state — it will be used
	// when the engine supports state stores.
	_ = state

	var opts []engine.EngineOption
	opts = append(opts, engine.WithEmitter(emitter))
	opts = append(opts, engine.WithSafety(safety))

	engCfg := defaultEngineConfig()

	configYAML := os.Getenv("CHAOS_CONFIG_YAML")
	if configYAML != "" {
		chaosConfig, err := config.LoadFromBytes([]byte(configYAML))
		if err != nil {
			log.Fatalf("parse CHAOS_CONFIG_YAML: %v", err)
		}
		if err := chaosConfig.Validate(); err != nil {
			log.Fatalf("validate config: %v", err)
		}
		asserter, err := chaosConfig.BuildAsserter()
		if err != nil {
			log.Fatalf("build asserter: %v", err)
		}
		if asserter != nil {
			opts = append(opts, engine.WithAsserter(asserter))
			engCfg.AssertWait = true
			engCfg.AssertPollInterval = types.Duration{Duration: time.Second}
		}
	}

	registry := buildRegistry()

	scenarios, err := scenario.BuiltinCatalog()
	if err != nil {
		log.Fatalf("load catalog: %v", err)
	}

	eng := engine.New(
		engCfg,
		transport,
		registry,
		scenarios,
		opts...,
	)

	handler := chaosaws.NewProxyHandler(eng, transport, cfg.HoldPrefix)
	lambda.Start(handler.Handle)
}

// defaultEngineConfig returns the engine configuration for Lambda execution.
func defaultEngineConfig() types.EngineConfig {
	return types.EngineConfig{
		Mode: "deterministic",
	}
}

// buildRegistry creates a mutation registry with all built-in mutations.
func buildRegistry() *mutation.Registry {
	r := mutation.NewRegistry()
	for _, m := range []mutation.Mutation{
		&mutation.DelayMutation{},
		&mutation.DropMutation{},
		&mutation.CorruptMutation{},
		&mutation.DuplicateMutation{},
		&mutation.EmptyMutation{},
		&mutation.PartialMutation{},
		&mutation.SchemaDriftMutation{},
		&mutation.StaleReplayMutation{},
		&mutation.MultiDayMutation{},
		&mutation.CascadeDelayMutation{},
		&mutation.StaleSensorMutation{},
		&mutation.PhantomTriggerMutation{},
		&mutation.TriggerTimeoutMutation{},
		&mutation.FalseSuccessMutation{},
		&mutation.JobKillMutation{},
		&mutation.RollingDegradationMutation{},
		&mutation.PhantomSensorMutation{},
		&mutation.SplitSensorMutation{},
		&mutation.SlowWriteMutation{},
		&mutation.StreamingLagMutation{},
	} {
		if err := r.Register(m); err != nil {
			log.Fatalf("register mutation: %v", err)
		}
	}
	return r
}

// requireEnv reads an environment variable or exits with an error message.
func requireEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("required environment variable %s is not set", key)
	}
	return val
}

// envOrDefault reads an environment variable, returning fallback if empty.
func envOrDefault(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}
