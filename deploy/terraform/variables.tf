variable "region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "staging_bucket_name" {
  description = "Name of the existing S3 staging bucket (objects land here first)"
  type        = string
}

variable "pipeline_bucket_name" {
  description = "Name of the existing S3 pipeline bucket (final destination)"
  type        = string
}

variable "table_name" {
  description = "DynamoDB table name for chaos-data state"
  type        = string
  default     = "chaos-data"
}

variable "event_bus_name" {
  description = "EventBridge bus name for chaos events"
  type        = string
  default     = "default"
}

variable "proxy_lambda_zip_path" {
  description = "Local path to the proxy Lambda deployment zip"
  type        = string
}

variable "release_lambda_zip_path" {
  description = "Local path to the release Lambda deployment zip"
  type        = string
}

variable "release_schedule_expression" {
  description = "Schedule expression for the release Lambda (e.g. rate(5 minutes))"
  type        = string
  default     = "rate(5 minutes)"
}

variable "hold_prefix" {
  description = "S3 key prefix used for held-back objects"
  type        = string
  default     = "chaos-hold/"
}

variable "tags" {
  description = "Tags applied to all resources"
  type        = map(string)
  default     = {}
}

variable "chaos_config_yaml" {
  description = "YAML configuration for chaos-data asserter adapters (Dagster/Airflow). Passed as CHAOS_CONFIG_YAML environment variable to the proxy Lambda."
  type        = string
  default     = ""
}
