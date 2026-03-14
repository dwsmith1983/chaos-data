# Reference existing staging bucket — created outside this module.
data "aws_s3_bucket" "staging" {
  bucket = var.staging_bucket_name
}

# Trigger proxy Lambda on every new object in the staging bucket.
resource "aws_s3_bucket_notification" "staging" {
  bucket = data.aws_s3_bucket.staging.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.proxy.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.proxy_s3]
}
