provider "aws" {
  region = "us-east-1"
}



data "aws_caller_identity" "current" {}

variable region {
  type        = string
  default     = "us-east-1"
  description = "description"
}


variable ingestion_image {
  type        = string
  description = "description"
}

variable s3_naming_prefix {
  type        = string
  description = "description"
}


######################
# S3 Buckets
######################
resource "aws_s3_bucket" "coingecko_data" {
  bucket        = "${var.s3_naming_prefix}-coingecko-data-pipeline"
  force_destroy = true
}

resource "aws_s3_bucket" "athena_output" {
  bucket        = "${var.s3_naming_prefix}-coingecko-athena-results"
  force_destroy = true
}

######################
# AWS Secrets
######################

resource "aws_secretsmanager_secret" "coingecko" {
  name = "coingecko/api_key"
}


######################
# IAM Roles & Policies
######################
resource "aws_iam_role" "lambda_exec" {
  name = "coingecko_lambda_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_policy" "lambda_policy" {
  name = "coingecko_lambda_policy"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetAuthorizationToken"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "secretsmanager:*"
        ],
        Resource = aws_secretsmanager_secret.coingecko.arn
      },
      {
        Effect = "Allow",
        Action = [
          "logs:*",
          "s3:*",
          "athena:*",
          "glue:*"
        ],
        Resource = "*"
      }
    ]
  })
}


resource "aws_iam_role_policy_attachment" "lambda_attach" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_policy.arn

}

data "aws_ecr_repository" "ingest" {
  name = "coingecko-ingest"
}

data "aws_ecr_image" "latest" {
  repository_name = data.aws_ecr_repository.ingest.name
  image_tag       = "latest"
}

######################
# Lambda Function
######################
resource "aws_lambda_function" "ingest_snapshot" {
  function_name = "coingecko_snapshot_ingest"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"

  # Use full image URI with digest (repo_url@sha256:...)
  image_uri = "${data.aws_ecr_repository.ingest.repository_url}@${data.aws_ecr_image.latest.image_digest}"

  source_code_hash = data.aws_ecr_image.latest.image_digest

  timeout     = 60
  memory_size = 512

  environment {
    variables = {
      S3_BUCKET = aws_s3_bucket.coingecko_data.bucket
      COINGECKO_API_KEY = "arn:aws:secretsmanager:${var.region}:${data.aws_caller_identity.current.account_id}:secret:coingecko/api_key:api_key::"
    }
  }
}

######################
# EventBridge Rule (Daily Trigger)
######################
resource "aws_cloudwatch_event_rule" "daily_snapshot" {
  name                = "coingecko_daily_snapshot"
  schedule_expression = "cron(55 23 * * ? *)"
}

resource "aws_cloudwatch_event_target" "snapshot_lambda" {
  rule      = aws_cloudwatch_event_rule.daily_snapshot.name
  target_id = "snapshot_lambda"
  arn       = aws_lambda_function.ingest_snapshot.arn
}

resource "aws_lambda_permission" "allow_eventbridge_ingest" {
  statement_id  = "AllowExecutionFromEventBridgeIngest"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest_snapshot.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_snapshot.arn
}


resource "aws_glue_catalog_database" "coingecko" {
  name = "coingecko"
}

resource "aws_glue_catalog_table" "coingecko_raw" {
  name          = "coingecko_raw"
  database_name = aws_glue_catalog_database.coingecko.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.coingecko_data.bucket}/raw/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "name"
      type = "string"
    }
    columns {
      name = "current_price"
      type = "double"
    }
    columns {
      name = "market_cap"
      type = "double"
    }
    columns {
      name = "circulating_supply"
      type = "double"
    }
    columns {
      name = "total_supply"
      type = "double"
    }
    columns {
      name = "last_updated"
      type = "timestamp"
    }
    columns {
      name = "ath"
      type = "double"
    }
    columns {
      name = "atl"
      type = "double"
    }
    columns {
      name = "roi"
      type = "double"
    }
    columns {
      name = "price_change_percentage_1h_in_currency"
      type = "double"
    }
    columns {
      name = "price_change_percentage_24h_in_currency"
      type = "double"
    }
    columns {
      name = "price_change_percentage_7d_in_currency"
      type = "double"
    }
    columns {
      name = "total_volume"
      type = "double"
    }
    columns {
      name = "high_24h"
      type = "double"
    }
    columns {
      name = "low_24h"
      type = "double"
    }
    columns {
      name = "inserted_at"
      type = "timestamp"
    }

    compressed        = false
    stored_as_sub_directories = false
  }
}
