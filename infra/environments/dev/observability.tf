# Alerts for the failed step: Lambda errors (CloudWatch alarms), Glue job failures
# (EventBridge) and Airflow task failures (published by the DAG's failure callback in aws mode).

resource "aws_sns_topic" "alerts" {
  #checkov:skip=CKV_AWS_26:Synthetic lab alerts only; alias/aws/sns blocks CloudWatch/EventBridge publishers, and a compatible customer-managed key adds recurring cost. See docs/TERRAFORM.md.
  name = "${var.name_prefix}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count = var.alert_email == null ? 0 : 1

  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_sns_topic_policy" "alerts" {
  arn = aws_sns_topic.alerts.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EventBridgeGlueFailures"
        Effect    = "Allow"
        Principal = { Service = "events.amazonaws.com" }
        Action    = "sns:Publish"
        Resource  = aws_sns_topic.alerts.arn
        # EventBridge SNS targets do not support Condition blocks in the topic policy.
        # The service principal and exact topic are scoped; see AWS resource-based policy docs.
      },
      {
        Sid       = "CloudWatchAlarms"
        Effect    = "Allow"
        Principal = { Service = "cloudwatch.amazonaws.com" }
        Action    = "sns:Publish"
        Resource  = aws_sns_topic.alerts.arn
        Condition = { StringEquals = { "aws:SourceAccount" = local.account_id } }
      },
    ]
  })
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  for_each = local.file_sources

  alarm_name          = "${var.name_prefix}-${each.value.function}-errors"
  alarm_description   = "Ingestion Lambda ${each.key} raised an error"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.ingest[each.key].function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_event_rule" "glue_failed" {
  name        = "${var.name_prefix}-glue-job-failed"
  description = "Case 01 Glue job ended in FAILED/TIMEOUT/ERROR"
  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail = {
      jobName = [for j in aws_glue_job.this : j.name]
      state   = ["FAILED", "TIMEOUT", "ERROR"]
    }
  })
}

resource "aws_cloudwatch_event_target" "glue_failed_sns" {
  rule = aws_cloudwatch_event_rule.glue_failed.name
  arn  = aws_sns_topic.alerts.arn
}
