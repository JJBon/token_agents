terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = ">= 5.40.0" }
  }
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}


locals {
  topic_name = "${var.name_prefix}-alerts"
  sfn_name   = "${var.name_prefix}-run-ecs"
}

resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/states/${local.sfn_name}"
  retention_in_days = 30
  tags              = var.tags
}

resource "aws_security_group" "ecs_task" {
  name        = "${var.name_prefix}-ecs-task-sg"
  description = "Security Group for ECS task launched by Step Functions"
  vpc_id      = var.vpc_id
  tags        = var.tags

  # Egress only (allow all outbound)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  ingress {
  description = "Allow HTTP from VPC"
  from_port   = 80
  to_port     = 80
  protocol    = "tcp"
  cidr_blocks = [var.cidr_block] # or a specific CIDR
    }
}

# SNS topic + email subscription
resource "aws_sns_topic" "alerts" {
  name = local.topic_name
  tags = var.tags
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.notification_email
}

# Step Functions role
resource "aws_iam_role" "sfn_role" {
  name               = "${var.name_prefix}-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "sfn_assume" {
  statement {
    effect = "Allow"
    principals { 
        type = "Service"
        identifiers = ["states.amazonaws.com"] 
    }
    actions   = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "sfn_inline" {
  statement {
    sid     = "ECSRun"
    effect  = "Allow"
    actions = ["ecs:RunTask","ecs:StopTask","ecs:DescribeTasks"]
    resources = ["*"]
  }

  # Allow passing roles referenced by the task definition (harmless if not needed)
  statement {
    sid     = "PassRoles"
    effect  = "Allow"
    actions = ["iam:PassRole"]
    resources = ["*"]
  }

  statement {
    sid     = "SNS"
    effect  = "Allow"
    actions = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }

  statement {
    sid     = "Logs"
    effect  = "Allow"
    actions = ["logs:CreateLogDelivery","logs:GetLogDelivery","logs:UpdateLogDelivery","logs:DeleteLogDelivery","logs:ListLogDeliveries"]
    resources = ["*"]
  }

  statement {
    sid     = "LogsDescribeAndPolicy"
    effect  = "Allow"
    actions = [
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams",
      "logs:DescribeResourcePolicies",
      "logs:PutResourcePolicy"
    ]
    resources = ["*"]
  }

  statement {
    sid     = "EventsManagedRule"
    effect  = "Allow"
    actions = [
      "events:PutRule",
      "events:PutTargets",
      "events:DescribeRule",
      "events:DeleteRule",
      "events:RemoveTargets",
      "events:TagResource"
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:events:*:${data.aws_caller_identity.current.account_id}:rule/StepFunctionsGetEventsForECSTaskRule",
      "arn:${data.aws_partition.current.partition}:events:*:${data.aws_caller_identity.current.account_id}:rule/*"
    ]
  }


    statement {
    sid     = "LogsWriteToSfnGroup"
    effect  = "Allow"
    actions = [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
    ]
    resources = ["${aws_cloudwatch_log_group.sfn.arn}:*"]
    }

    statement {
    sid     = "CreateEventsSLR"
    effect  = "Allow"
    actions = ["iam:CreateServiceLinkedRole"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "iam:AWSServiceName"
      values   = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_policy" "sfn_policy" {
  name   = "${var.name_prefix}-sfn-inline"
  policy = data.aws_iam_policy_document.sfn_inline.json
}

resource "aws_iam_role_policy_attachment" "sfn_attach" {
  role       = aws_iam_role.sfn_role.name
  policy_arn = aws_iam_policy.sfn_policy.arn
}

# State machine definition
locals {
  sfn_definition = jsonencode({
    Comment = "Run ECS task and notify on failure"
    StartAt = "RunECS"
    States = {
      RunECS = {
        Type     = "Task"
        Resource = "arn:aws:states:::ecs:runTask.sync"
        Parameters = {
          Cluster        = var.ecs_cluster_arn
          TaskDefinition = var.ecs_task_definition_arn
          LaunchType     = "FARGATE"
          NetworkConfiguration = {
            AwsvpcConfiguration = {
              Subnets        = var.subnet_ids
              SecurityGroups = [aws_security_group.ecs_task.id]
              AssignPublicIp ="ENABLED"
            }
          }
        }
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        End = true
      }
      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.alerts.arn
          Subject  = "🚨 LangGraph ECS task FAILED"
          "Message.$" = "States.JsonToString($)"
        }
        End = true
      }
    }
  })
}

resource "aws_sfn_state_machine" "this" {
  name       = local.sfn_name
  role_arn   = aws_iam_role.sfn_role.arn
  definition = local.sfn_definition

  logging_configuration {
    include_execution_data = true
    level                  = "ERROR"
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*" 
  }

  tags = var.tags
}

# Optional: EventBridge Scheduler to trigger the SFN daily
resource "aws_iam_role" "scheduler_role" {
  count              = var.schedule_cron == null ? 0 : 1
  name               = "${var.name_prefix}-scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    effect = "Allow"
    principals { 
    type = "Service"
    identifiers = ["scheduler.amazonaws.com"] 
    }
    actions   = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role_policy" "scheduler_invoke" {
  count  = var.schedule_cron == null ? 0 : 1
  name   = "${var.name_prefix}-scheduler-invoke"
  role   = aws_iam_role.scheduler_role[0].id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["states:StartExecution"],
      Resource = aws_sfn_state_machine.this.arn
    }]
  })
}

resource "aws_scheduler_schedule" "daily" {
  count                        = var.schedule_cron == null ? 0 : 1
  name                         = "${var.name_prefix}-daily"
  schedule_expression          = var.schedule_cron
  schedule_expression_timezone = var.schedule_timezone
  flexible_time_window { mode  = "OFF" }

  target {
    arn      = aws_sfn_state_machine.this.arn
    role_arn = aws_iam_role.scheduler_role[0].arn
    input    = jsonencode({}) # empty input
  }
}
