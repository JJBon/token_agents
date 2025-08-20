#############################################
# OpenSearch Serverless policies + collection
#############################################

resource "aws_opensearchserverless_security_policy" "kb_encryption" {
  name        = "${var.s3_naming_prefix}-kb-encryption"
  type        = "encryption"
  description = "Encryption policy for KB vector collection"
  policy = jsonencode({
    Rules = [{
      ResourceType = "collection",
      Resource     = ["collection/${var.s3_naming_prefix}-kb"]
    }]
    AWSOwnedKey = true
  })
}

resource "aws_opensearchserverless_security_policy" "kb_network" {
  name        = "${var.s3_naming_prefix}-kb-network"
  type        = "network"
  description = "Network policy for KB vector collection"
  policy = jsonencode([{
    Description = "Network policy for KB collection"
    Rules = [{
      ResourceType    = "collection",
      Resource        = ["collection/${var.s3_naming_prefix}-kb"]
      AllowFromPublic = var.allow_public_network
    }]
  }])
}

resource "aws_opensearchserverless_collection" "kb" {
  name        = "${var.s3_naming_prefix}-kb"
  type        = "VECTORSEARCH"
  description = "Vector collection for Bedrock KB"
  depends_on  = [
    aws_opensearchserverless_security_policy.kb_encryption,
    aws_opensearchserverless_security_policy.kb_network
  ]
}

##########################
# IAM role for Bedrock KB
##########################

resource "aws_iam_role" "bedrock_kb_role" {
  name = "${var.s3_naming_prefix}-bedrock-kb-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect    = "Allow",
      Principal = { Service = "bedrock.amazonaws.com" },
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_policy" "bedrock_kb_policy" {
  name = "${var.s3_naming_prefix}-bedrock-kb-policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      # Read source documents (bucket + prefixes)
      {
        Effect = "Allow",
        Action = ["s3:ListBucket"],
        Resource = var.s3_bucket_arn
      },
      {
        Effect = "Allow",
        Action = ["s3:GetObject"],
        Resource = [
          for p in var.s3_inclusion_prefixes :
          "${var.s3_bucket_arn}/${p}*"
        ]
      },
      # Invoke embedding model
      {
        Effect   = "Allow",
        Action   = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
        Resource = [var.kb_embedding_model_arn]
      },
      # AOSS data-plane API (further constrained by AOSS data access policy below)
      {
        Effect   = "Allow",
        Action   = ["aoss:APIAccessAll"],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "bedrock_kb_attach" {
  role       = aws_iam_role.bedrock_kb_role.name
  policy_arn = aws_iam_policy.bedrock_kb_policy.arn
}

##############################################
# AOSS data access policy (role-scoped access)
##############################################

resource "aws_opensearchserverless_access_policy" "kb_data" {
  name        = "${var.s3_naming_prefix}-kb-data"
  type        = "data"
  description = "Data access policy for Bedrock KB role"
  policy = jsonencode([{
    Description = "KB role access to collection and indexes"
    Principal   = [aws_iam_role.bedrock_kb_role.arn]
    Rules = [
      {
        ResourceType = "collection"
        Resource     = ["collection/${aws_opensearchserverless_collection.kb.name}"]
        Permission   = ["aoss:DescribeCollectionItems"]
      },
      {
        ResourceType = "index"
        Resource     = ["index/${aws_opensearchserverless_collection.kb.name}/*"]
        Permission   = [
          "aoss:CreateIndex",
          "aoss:UpdateIndex",
          "aoss:DescribeIndex",
          "aoss:ReadDocument",
          "aoss:WriteDocument"
        ]
      }
    ]
  }])
}

#############################################
# Bedrock Knowledge Base + S3 Data Source
#############################################

resource "aws_bedrockagent_knowledge_base" "kb" {
  name     = var.kb_name
  role_arn = aws_iam_role.bedrock_kb_role.arn

  knowledge_base_configuration {
    type = "VECTOR"
    vector_knowledge_base_configuration {
      embedding_model_arn = var.kb_embedding_model_arn
    }
  }

  storage_configuration {
    type = "OPENSEARCH_SERVERLESS"
    opensearch_serverless_configuration {
      collection_arn    = aws_opensearchserverless_collection.kb.arn
      vector_index_name = "bedrock-knowledge-base-default-index"
      field_mapping {
        vector_field   = "bedrock-knowledge-base-default-vector"
        text_field     = "AMAZON_BEDROCK_TEXT_CHUNK"
        metadata_field = "AMAZON_BEDROCK_METADATA"
      }
    }
  }

  depends_on = [aws_opensearchserverless_access_policy.kb_data]

  tags = var.tags
}

resource "aws_bedrockagent_data_source" "kb_s3" {
  knowledge_base_id = aws_bedrockagent_knowledge_base.kb.id
  name              = "${var.kb_name}-s3-datasource"

  data_source_configuration {
    type = "S3"
    s3_configuration {
      bucket_arn         = var.s3_bucket_arn
      inclusion_prefixes = var.s3_inclusion_prefixes
    }
  }
}
