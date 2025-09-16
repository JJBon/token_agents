output "state_machine_arn" { value = aws_sfn_state_machine.this.arn }
output "sns_topic_arn"     { value = aws_sns_topic.alerts.arn }
output "schedule_name"     { value = try(aws_scheduler_schedule.daily[0].name, null) }
