resource "aws_cloudwatch_event_rule" "release_schedule" {
  name                = "${var.table_name}-release-schedule"
  description         = "Triggers the chaos-data release Lambda on a schedule"
  schedule_expression = var.release_schedule_expression
  tags                = var.tags
}

resource "aws_cloudwatch_event_target" "release_lambda" {
  rule = aws_cloudwatch_event_rule.release_schedule.name
  arn  = aws_lambda_function.release.arn
}

resource "aws_lambda_permission" "release_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.release.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.release_schedule.arn
}
