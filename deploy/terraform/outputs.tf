output "proxy_lambda_arn" {
  description = "ARN of the proxy Lambda function"
  value       = aws_lambda_function.proxy.arn
}

output "release_lambda_arn" {
  description = "ARN of the release Lambda function"
  value       = aws_lambda_function.release.arn
}

output "dynamodb_table_name" {
  description = "Name of the DynamoDB table"
  value       = aws_dynamodb_table.chaos_data.name
}

output "proxy_lambda_function_name" {
  description = "Name of the proxy Lambda function"
  value       = aws_lambda_function.proxy.function_name
}

output "release_lambda_function_name" {
  description = "Name of the release Lambda function"
  value       = aws_lambda_function.release.function_name
}
