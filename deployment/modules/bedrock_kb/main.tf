resource "aws_iam_role" "kb_role" {
  name = "${var.name}-kb-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = { Service = "bedrock.amazonaws.com" },
      Action = "sts:AssumeRole"
    }]
  })
}

provider "time" {}

resource "time_sleep" "after_role" {
  depends_on      = [aws_iam_role.kb_role, aws_iam_role_policy_attachment.attach]
  create_duration = "30s"  # 20–60s is usually enough
}

resource "aws_iam_policy" "kb_policy" {
  name_prefix = "${var.name}-kb-"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      # S3 read for ingestion (research/news data sources)
      {
        Effect   = "Allow",
        Action   = ["s3:GetObject","s3:ListBucket"],
        Resource = [var.source_bucket_arn, "${var.source_bucket_arn}/*"]
      },

      # Read DB secret (+ decrypt via Secrets Manager)
      {
        Effect   = "Allow",
        Action   = ["secretsmanager:GetSecretValue","secretsmanager:DescribeSecret"],
        Resource = var.rds_secret_arn
      },
      {
        Effect = "Allow",
        Action = ["kms:Decrypt"],
        Resource = "*",
        Condition = {
          "ForAnyValue:StringEquals" = {
            "kms:ViaService" = ["secretsmanager.${data.aws_region.current.name}.amazonaws.com"]
          }
        }
      },

      # 🔧 REQUIRED for KB with RDS storage (use * for Describe — RDS often ignores ARNs here)
      {
        Effect = "Allow",
        Action = [
          "rds:DescribeDBClusters",
          "rds:DescribeDBInstances",
          "rds:DescribeDBClusterEndpoints",
          "rds:DescribeDBSubnetGroups",
          "rds:ListTagsForResource",
          "rds-data:ExecuteStatement",
          "rds-data:BatchExecuteStatement" 
        ],
        Resource = "*"
      },
      {
        Effect   = "Allow",
        Action   = ["bedrock:InvokeModel"],
        Resource = "*"
      },
       {
        Effect   = "Allow",
        Action   = ["bedrock:ListFoundationModels","bedrock:ListCustomModels"],
        Resource = "*"
      },
      {
        "Effect": "Allow",
        "Action": [
          "bedrock-agent:StartIngestionJob",
          "bedrock-agent:GetIngestionJob",
          "bedrock-agent:ListIngestionJobs",
          "bedrock-agent-runtime:Retrieve"
        ],
        "Resource": "*"
      }
    ]
  })
}

data "aws_region" "current" {}

resource "aws_iam_role_policy_attachment" "attach" {
  role       = aws_iam_role.kb_role.name
  policy_arn = aws_iam_policy.kb_policy.arn
}

# Knowledge Base (RDS/pgvector as storage)
resource "awscc_bedrock_knowledge_base" "this" {
  name     = var.name
  role_arn = aws_iam_role.kb_role.arn
  depends_on = [time_sleep.after_role]

  knowledge_base_configuration = {
    type = "VECTOR"
    vector_knowledge_base_configuration = {
      embedding_model_arn = var.embedding_model_arn
    }
  }

  storage_configuration = {
    type = "RDS"
    rds_configuration = {
      resource_arn          = var.aurora_cluster_arn
      credentials_secret_arn= var.rds_secret_arn
      database_name         = var.database_name
      table_name            = var.table_name
      field_mapping = {
        primary_key_field   = var.pk_field
        text_field          = var.text_field
        vector_field        = var.vector_field
        metadata_field      = var.metadata_field
        custom_metadata_field = var.custom_metadata_field
      }
    }
  }
}

# Optional S3 data source connected to this KB (for ingestion)
resource "awscc_bedrock_data_source" "s3" {
  knowledge_base_id = awscc_bedrock_knowledge_base.this.knowledge_base_id
  name              = "${var.name}-s3"

  data_source_configuration = {
    type = "S3"
    s3_configuration = {
      bucket_arn          = var.source_bucket_arn
      inclusion_prefixes  = var.source_prefixes
    }
  }

  vector_ingestion_configuration = {
    chunking_configuration = {
      chunking_strategy = "FIXED_SIZE"
      fixed_size_chunking_configuration = {
        max_tokens          = var.chunk_max_tokens
        overlap_percentage  = var.chunk_overlap_pct
      }
    }
  }
}

output "kb_id"           { value = awscc_bedrock_knowledge_base.this.knowledge_base_id }
output "kb_role_arn"     { value = aws_iam_role.kb_role.arn }
output "data_source_id"  { value = awscc_bedrock_data_source.s3.data_source_id }