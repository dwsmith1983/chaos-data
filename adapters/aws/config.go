package aws

import "errors"

// Config holds AWS adapter configuration.
type Config struct {
	// Region is the AWS region. Defaults to "us-east-1".
	Region string

	// StagingBucket is the S3 bucket for staging data. Required.
	StagingBucket string

	// PipelineBucket is the S3 bucket for pipeline output. Required.
	PipelineBucket string

	// TableName is the DynamoDB table for state storage. Required.
	TableName string

	// EventBusName is the EventBridge bus name. Defaults to "default".
	EventBusName string

	// HoldPrefix is the S3 key prefix for held objects. Defaults to
	// "chaos-hold/".
	HoldPrefix string
}

// Validate checks that all required fields are set. It returns an error
// describing the first missing field found.
func (c *Config) Validate() error {
	if c.StagingBucket == "" {
		return errors.New("aws: StagingBucket is required")
	}
	if c.PipelineBucket == "" {
		return errors.New("aws: PipelineBucket is required")
	}
	if c.TableName == "" {
		return errors.New("aws: TableName is required")
	}
	return nil
}

// Defaults fills in default values for optional fields that are empty.
// Call Defaults before Validate to ensure optional fields have sensible
// values.
func (c *Config) Defaults() {
	if c.Region == "" {
		c.Region = "us-east-1"
	}
	if c.EventBusName == "" {
		c.EventBusName = "default"
	}
	if c.HoldPrefix == "" {
		c.HoldPrefix = "chaos-hold/"
	}
}
