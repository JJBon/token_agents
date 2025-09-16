terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = { source = "hashicorp/aws", version = ">= 5.40.0" }
  }
}

locals {
  log_group_name = "/ecs/${var.name_prefix}"
  image          = "${var.ecr_image_url}:${var.image_tag}"
}

resource "aws_cloudwatch_log_group" "this" {
  name              = local.log_group_name
  retention_in_days = 30
}

resource "aws_ecs_cluster" "this" {
  name = "${var.name_prefix}-cluster"
  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

# Execution role (pulls from ECR + writes logs)
resource "aws_iam_role" "exec" {
  name               = "${var.name_prefix}-exec"
  assume_role_policy = data.aws_iam_policy_document.exec_assume.json
}

data "aws_iam_policy_document" "exec_assume" {
  statement {
    effect = "Allow"
    principals { 
        type = "Service"
        identifiers = ["ecs-tasks.amazonaws.com"] 
    }
    actions   = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role_policy_attachment" "exec_ecr" {
  role       = aws_iam_role.exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy_attachment" "exec_ecr_2" {
  role       = aws_iam_role.exec.name
  policy_arn = aws_iam_policy.task_base.arn
}

# Task role (your app permissions)
resource "aws_iam_role" "task" {
  name               = "${var.name_prefix}-task"
  assume_role_policy = data.aws_iam_policy_document.exec_assume.json
}

data "aws_iam_policy_document" "task_base" {
  statement {
    sid     = "S3"
    effect  = "Allow"
    actions = ["s3:GetObject","s3:PutObject","s3:ListBucket"]
    resources = ["*"]
  }

  statement {
    sid     = "GlueAthena"
    effect  = "Allow"
    actions = [
      "glue:GetDatabase","glue:GetDatabases","glue:CreateDatabase","glue:GetTable","glue:CreateTable","glue:UpdateTable","glue:DeleteTable",
      "athena:StartQueryExecution","athena:GetQueryExecution","athena:GetQueryResults"
    ]
    resources = ["*"]
  }

  statement {
    sid     = "BedrockKBIngestion"
    effect  = "Allow"
    actions = ["bedrock:StartIngestionJob","bedrock:ListIngestionJobs","bedrock:GetIngestionJob","bedrock:InvokeModel"]
    resources = ["*"]
  }

  statement {
    sid     = "Logs"
    effect  = "Allow"
    actions = ["logs:CreateLogStream","logs:PutLogEvents","logs:DescribeLogStreams"]
    resources = ["*"]
  }

  dynamic "statement" {
    for_each = length(var.secrets_mgr_arns) > 0 ? [1] : []
    content {
      sid     = "Secrets"
      effect  = "Allow"
      actions = ["secretsmanager:GetSecretValue"]
      resources = values(var.secrets_mgr_arns)
    }
  }
}

resource "aws_iam_policy" "task_extra" {
  count  = var.extra_task_policy_json == null ? 0 : 1
  name   = "${var.name_prefix}-task-extra"
  policy = var.extra_task_policy_json
}

resource "aws_iam_role_policy_attachment" "task_attach" {
  role       = aws_iam_role.task.name
  policy_arn = aws_iam_policy.task_base.arn
}

resource "aws_iam_policy" "task_base" {
  name   = "${var.name_prefix}-task-base"
  policy = data.aws_iam_policy_document.task_base.json
}

resource "aws_iam_role_policy_attachment" "task_extra_attach" {
  count      = var.extra_task_policy_json == null ? 0 : 1
  role       = aws_iam_role.task.name
  policy_arn = aws_iam_policy.task_extra[0].arn
}

# Task definition (no service; we will RunTask via SFN)
resource "aws_ecs_task_definition" "this" {
  family                   = "${var.name_prefix}-task"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.cpu
  memory                   = var.memory
  execution_role_arn       = aws_iam_role.exec.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([
    {
      name      = var.container_name
      image     = local.image
      essential = true
      workingDirectory = "/app"                                  # <-- if your code is under /app
      command   = ["python", "agents/news_agent/graph.py"]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.this.name
          awslogs-region        = data.aws_region.current.name
          awslogs-stream-prefix = "ecs"
        }
      }
      environment = concat([
        for k, v in var.env_vars : { name = k, value = v }
      ],  [{ name = "PYTHONPATH", value = "/app" }] 
      )
      secrets = [
        for k, arn in var.secrets_mgr_arns :
        { name = k, valueFrom = arn }
      ]
    }
  ])

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }
}

data "aws_region" "current" {}
