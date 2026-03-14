# -----------------------------------------------------------------------------
# Shared assume-role policy for Lambda
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# -----------------------------------------------------------------------------
# Proxy Lambda IAM
# -----------------------------------------------------------------------------

resource "aws_iam_role" "proxy_lambda" {
  name               = "${var.table_name}-proxy-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

data "aws_iam_policy_document" "proxy_lambda" {
  # S3 — object operations on staging and pipeline buckets
  statement {
    sid = "S3ObjectOps"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:CopyObject",
    ]
    resources = [
      "${data.aws_s3_bucket.staging.arn}/*",
      "arn:aws:s3:::${var.pipeline_bucket_name}/*",
    ]
  }

  # S3 — list staging bucket (for prefix scans)
  statement {
    sid       = "S3ListStaging"
    actions   = ["s3:ListBucket"]
    resources = [data.aws_s3_bucket.staging.arn]
  }

  # DynamoDB — read/write chaos state
  statement {
    sid = "DynamoDB"
    actions = [
      "dynamodb:PutItem",
      "dynamodb:GetItem",
    ]
    resources = [aws_dynamodb_table.chaos_data.arn]
  }

  # EventBridge — emit chaos events
  statement {
    sid       = "EventBridge"
    actions   = ["events:PutEvents"]
    resources = ["arn:aws:events:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:event-bus/${var.event_bus_name}"]
  }

  # CloudWatch Logs
  statement {
    sid = "Logs"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.table_name}-proxy:*"]
  }
}

resource "aws_iam_role_policy" "proxy_lambda" {
  name   = "${var.table_name}-proxy-lambda"
  role   = aws_iam_role.proxy_lambda.id
  policy = data.aws_iam_policy_document.proxy_lambda.json
}

# -----------------------------------------------------------------------------
# Release Lambda IAM
# -----------------------------------------------------------------------------

resource "aws_iam_role" "release_lambda" {
  name               = "${var.table_name}-release-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

data "aws_iam_policy_document" "release_lambda" {
  # S3 — full object operations on pipeline bucket
  statement {
    sid = "S3PipelineOps"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:CopyObject",
      "s3:ListBucket",
    ]
    resources = [
      "arn:aws:s3:::${var.pipeline_bucket_name}",
      "arn:aws:s3:::${var.pipeline_bucket_name}/*",
    ]
  }

  # S3 — read-only on staging bucket for holdback reads
  statement {
    sid       = "S3StagingRead"
    actions   = ["s3:GetObject"]
    resources = ["${data.aws_s3_bucket.staging.arn}/*"]
  }

  # DynamoDB — read chaos state
  statement {
    sid       = "DynamoDB"
    actions   = ["dynamodb:GetItem"]
    resources = [aws_dynamodb_table.chaos_data.arn]
  }

  # CloudWatch Logs
  statement {
    sid = "Logs"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.table_name}-release:*"]
  }
}

resource "aws_iam_role_policy" "release_lambda" {
  name   = "${var.table_name}-release-lambda"
  role   = aws_iam_role.release_lambda.id
  policy = data.aws_iam_policy_document.release_lambda.json
}
