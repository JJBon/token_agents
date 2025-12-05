terraform {
  required_version = ">= 1.6"
}

locals {
  region = "us-east-1"
}

provider "aws" {
  region = local.region
}

provider "awscc" { region = "us-east-1" }

data "aws_caller_identity" "current" {}
data "aws_ecr_repository" "ingest" {
  name = "coingecko-ingest"
}

data "aws_ecr_image" "latest" {
  repository_name = data.aws_ecr_repository.ingest.name
  image_tag       = "latest"
}

data "aws_vpc" "selected" {
  # either default = true OR filter by tag
  default = true
}

data "aws_subnets" "private" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.selected.id]
  }

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

  timeout     = 360
  memory_size = 512

  environment {
    variables = {
      S3_BUCKET         = aws_s3_bucket.coingecko_data.bucket
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

resource "aws_glue_catalog_database" "news_agent" {
  name = "news_agent"
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

    compressed                = false
    stored_as_sub_directories = false
  }
}

module "aurora_pg" {
  source        = "./modules/aurora_pgvector"
  name          = "${var.s3_naming_prefix}-kb-pg"
  vpc_id        = data.aws_vpc.selected.id
  subnet_ids    = data.aws_subnets.private.ids
  db_name       = "kbdb"
  min_acu       = 0
  max_acu       = 4
  allowed_cidrs = [] # usually empty; KB connects via service role + secret
}


resource "null_resource" "wait_data_api" {
  triggers = { cluster_arn = module.aurora_pg.cluster_arn }
  depends_on = [module.aurora_pg]

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    environment = {
      CLUSTER_ARN = self.triggers.cluster_arn
      AWS_REGION  = var.region
    }
    command = <<EOT
set -euo pipefail

# Get the bit after ':cluster:' safely, without  expansion
CLUSTER_ID="$(printf '%s\n' "$CLUSTER_ARN" | awk -F: '{print $NF}')"

for i in $(seq 1 60); do
  ready="$(aws rds describe-db-clusters \
    --db-cluster-identifier "$CLUSTER_ID" \
    --query 'DBClusters[0].HttpEndpointEnabled' \
    --output text 2>/dev/null || true)"

  if [ "$ready" = "True" ]; then
    echo "✅ Data API ready on $CLUSTER_ID"
    exit 0
  fi

  echo "⏳ Waiting for Data API on $CLUSTER_ID... ($i/60)"
  sleep 10
done

echo "❌ Timed out waiting for Data API on $CLUSTER_ID" >&2
exit 1
EOT
  }
}

resource "null_resource" "bootstrap_pgvector" {
  depends_on = [null_resource.wait_data_api]

  triggers = {
    cluster_arn = module.aurora_pg.cluster_arn
    secret_arn  = module.aurora_pg.secret_arn
    db          = module.aurora_pg.db_name
    dim         = tostring(var.kb_vector_dimension)
  }

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    environment = {
      CLUSTER_ARN = self.triggers.cluster_arn
      SECRET_ARN  = self.triggers.secret_arn
      DB          = self.triggers.db
      DIM         = self.triggers.dim
    }
    command = <<EOT
set -euo pipefail

execsql() {
  local sql="$1"
  local tries=0
  local max=40      # ~200s worst case
  local sleep_s=5
  while true; do
    set +e
    out=$(aws rds-data execute-statement \
      --resource-arn "$CLUSTER_ARN" \
      --secret-arn   "$SECRET_ARN" \
      --database     "$DB" \
      --sql "$sql" 2>&1)
    rc=$?
    set -e
    if [ $rc -eq 0 ]; then
      break
    fi
    if echo "$out" | grep -qiE 'DatabaseResumingException|Communications|timeout|Throttl|BadGateway|ServiceUnavailable|database is starting up'; then
      tries=$((tries+1))
      if [ $tries -ge $max ]; then
        echo "❌ Gave up after $tries attempts: $out" >&2
        exit 1
      fi
      echo "⏳ DB resuming or transient error; retry $tries/$max..."
      sleep $sleep_s
      continue
    fi
    echo "❌ Non-retryable error: $out" >&2
    exit 1
  done
}

# Warm up: keep pinging until SELECT 1 works
execsql "SELECT 1;"

# 1) Extension
execsql "CREATE EXTENSION IF NOT EXISTS vector;"

# 2) Tables
execsql "CREATE TABLE IF NOT EXISTS public.research_kb (
  id uuid PRIMARY KEY,
  chunks TEXT,
  embedding VECTOR($DIM),
  metadata JSONB,
  custom_metadata JSONB
);"

execsql "CREATE TABLE IF NOT EXISTS public.news_kb (
  id uuid PRIMARY KEY,
  chunks TEXT,
  embedding VECTOR($DIM),
  metadata JSONB,
  custom_metadata JSONB
);"

# 3) Required indexes for Bedrock KB
execsql "CREATE INDEX IF NOT EXISTS research_kb_hnsw
  ON public.research_kb USING hnsw (embedding vector_cosine_ops);"
execsql "CREATE INDEX IF NOT EXISTS news_kb_hnsw
  ON public.news_kb USING hnsw (embedding vector_cosine_ops);"

execsql "CREATE INDEX IF NOT EXISTS research_kb_chunks_tsv_gin
  ON public.research_kb USING gin (to_tsvector('simple', chunks));"
execsql "CREATE INDEX IF NOT EXISTS news_kb_chunks_tsv_gin
  ON public.news_kb USING gin (to_tsvector('simple', chunks));"

execsql "CREATE INDEX IF NOT EXISTS research_kb_custom_md_gin
  ON public.research_kb USING gin (custom_metadata);"
execsql "CREATE INDEX IF NOT EXISTS news_kb_custom_md_gin
  ON public.news_kb USING gin (custom_metadata);"
EOT
  }
}


# --- Research KB (S3 -> Aurora/pgvector) ---
module "kb_research_pg" {
  source                = "./modules/bedrock_kb"
  name                  = "${var.s3_naming_prefix}-kb-research"
  aurora_cluster_arn    = module.aurora_pg.cluster_arn
  rds_secret_arn        = module.aurora_pg.secret_arn
  database_name         = module.aurora_pg.db_name
  table_name            = "public.research_kb" # your pg table (created once)
  pk_field              = "id"
  text_field            = "chunks"
  vector_field          = "embedding"
  metadata_field        = "metadata"
  custom_metadata_field = "custom_metadata"

  source_bucket_arn = aws_s3_bucket.research_docs.arn
  source_prefixes   = ["research/"]
}

# --- News KB (S3 -> Aurora/pgvector) ---
module "kb_news_pg" {
  source                = "./modules/bedrock_kb"
  name                  = "${var.s3_naming_prefix}-kb-news"
  aurora_cluster_arn    = module.aurora_pg.cluster_arn
  rds_secret_arn        = module.aurora_pg.secret_arn
  database_name         = module.aurora_pg.db_name
  table_name            = "public.news_kb"
  pk_field              = "id"
  text_field            = "chunks"
  vector_field          = "embedding"
  metadata_field        = "metadata"
  custom_metadata_field = "custom_metadata"

  source_bucket_arn = aws_s3_bucket.news_docs.arn
  source_prefixes   = ["news/"]
}

# Example S3 buckets for the two Kbs:
resource "aws_s3_bucket" "research_docs" { bucket = "${var.s3_naming_prefix}-research-kb" }
resource "aws_s3_bucket" "news_docs" { bucket = "${var.s3_naming_prefix}-news-kb" } 


module "ecs_news_ingest" {
  source = "./modules/ecs_fargate_task"

  name_prefix        = "news-ingest"
  vpc_id             = data.aws_vpc.selected.id
  subnet_ids         = data.aws_subnets.private.ids

  ecr_image_url = aws_ecr_repository.news_ingest_langgraph.repository_url     # from your existing ECR repo

  env_vars = {
    AWS_REGION              = local.region
    S3_BUCKET               = aws_s3_bucket.coingecko_data.bucket  
    GLUE_DATABASE           = "news_agent"
    NEWS_KB_ID              = module.kb_news_pg.kb_id
    NEWS_KB_DS_ID           = module.kb_news_pg.data_source_id
    NEWS_KB_BUCKET          = aws_s3_bucket.news_docs.bucket
    NEWS_KB_PREFIX          = "news/"
    RESEARCH_KB_ID          = module.kb_research_pg.kb_id
    AURORA_CLUSTER_ARN      = module.aurora_pg.cluster_arn
    AURORA_SECRET_ARN       = module.aurora_pg.secret_arn
    AURORA_DB_NAME          = module.aurora_pg.db_name
    RESEARCH_TABLE          = "public.research_kb"
    CRYPTONEWS_URL          = var.cryptonews_url   # or inject via secrets_mgr_arns
    MAX_ARTICLES            = "50"
    TIMEOUT_S               = "15"
    EXTRACTOR_TEMPERATURE   = "0.3"
    WAIT_FOR_INGEST         = "true"
    ATHENA_USE_MANAGED_RESULTS = "true"
  }

  secrets_mgr_arns = {
    CRYPTONEWS_TOKEN = aws_secretsmanager_secret.cryptonews_token.arn
  }


  s3_bucket_arns = [
    "arn:aws:s3:::${aws_s3_bucket.news_docs.bucket}",
  ]
}


module "sfn_run_news" {
  source = "./modules/sfn_ecs_runner"

  name_prefix               = "news-ingest"
  ecs_cluster_arn           = module.ecs_news_ingest.cluster_arn
  ecs_task_definition_arn   = module.ecs_news_ingest.task_definition_arn
  subnet_ids                = data.aws_subnets.private.ids
  vpc_id                    = data.aws_vpc.selected.id
  assign_public_ip          = false
  cidr_block                = data.aws_vpc.selected.cidr_block  # usually empty; SFN connects via service role + secret 

  notification_email        = var.alert_email

  # Daily at 06:00 America/Bogota
  schedule_cron             = "cron(0 6 * * ? *)"
  schedule_timezone         = "America/Bogota"

  tags = {
    Project = "NewsIngest"
    Owner   = "Data"
  }
}

module "cognito_auth" {
  source = "./modules/cognito" # ← change to your module path
  region = var.region
  user_pool_name = "${var.s3_naming_prefix}-user-pool"
  project_prefix = var.s3_naming_prefix


  identity_pool_name = "${var.s3_naming_prefix}-identity-pool"


  create_hosted_ui_domain = true
  hosted_ui_domain_prefix = "${var.s3_naming_prefix}-cog-demo-auth" # must be globally unique


  callback_urls = [
  "http://localhost:3000/oauth/callback"
  ]
  logout_urls = [
    "http://localhost:3000/logout",
    "http://localhost:3000/"
  ]


  supported_identity_providers = ["COGNITO"]


  enable_bedrock_invoke = true
  bedrock_model_arns = [
  # Example Sonnet in us-east-1 — replace with what you actually allow
  "arn:aws:bedrock:us-east-1:123456789012:model/anthropic.claude-3-5-sonnet-20240620-v1:0"
  ]


  attach_basic_logs = true


  # Optional: map users in the "admins" group to a different role

}