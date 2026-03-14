# -----------------------------------------------------------------------------
# Proxy Lambda — intercepts objects arriving in the staging bucket
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "proxy" {
  function_name = "${var.table_name}-proxy"
  filename      = var.proxy_lambda_zip_path
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["x86_64"]
  role          = aws_iam_role.proxy_lambda.arn
  timeout       = 300
  memory_size   = 256

  source_code_hash = filebase64sha256(var.proxy_lambda_zip_path)

  environment {
    variables = {
      STAGING_BUCKET  = var.staging_bucket_name
      PIPELINE_BUCKET = var.pipeline_bucket_name
      TABLE_NAME      = var.table_name
      EVENT_BUS_NAME  = var.event_bus_name
      HOLD_PREFIX     = var.hold_prefix
    }
  }

  tags = var.tags
}

resource "aws_cloudwatch_log_group" "proxy" {
  name              = "/aws/lambda/${aws_lambda_function.proxy.function_name}"
  retention_in_days = 14
  tags              = var.tags
}

resource "aws_lambda_permission" "proxy_s3" {
  statement_id   = "AllowS3Invoke"
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.proxy.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = data.aws_s3_bucket.staging.arn
  source_account = data.aws_caller_identity.current.account_id
}

# -----------------------------------------------------------------------------
# Release Lambda — periodically releases held objects
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "release" {
  function_name = "${var.table_name}-release"
  filename      = var.release_lambda_zip_path
  handler       = "bootstrap"
  runtime       = "provided.al2023"
  architectures = ["x86_64"]
  role          = aws_iam_role.release_lambda.arn
  timeout       = 300
  memory_size   = 128

  source_code_hash = filebase64sha256(var.release_lambda_zip_path)

  environment {
    variables = {
      STAGING_BUCKET  = var.staging_bucket_name
      PIPELINE_BUCKET = var.pipeline_bucket_name
      TABLE_NAME      = var.table_name
      HOLD_PREFIX     = var.hold_prefix
    }
  }

  tags = var.tags
}

resource "aws_cloudwatch_log_group" "release" {
  name              = "/aws/lambda/${aws_lambda_function.release.function_name}"
  retention_in_days = 14
  tags              = var.tags
}
