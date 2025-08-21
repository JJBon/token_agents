#############################################
# OpenSearch Serverless policies + collection
#############################################

data "aws_caller_identity" "current" {}
data "aws_iam_session_context" "current" { arn = data.aws_caller_identity.current.arn }
data "aws_region" "current" {}


locals {
  account_root_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"

  kb_network_rule = merge(
    {
      Description     = "Network policy for KB collection"
      Rules           = [{
        ResourceType = "collection"
        Resource     = ["collection/${var.s3_naming_prefix}-kb"]
      }]
      AllowFromPublic = var.allow_public_network
    },
    # Only include private keys when not public
    var.allow_public_network ? {} : merge(
      length(var.source_vpce_ids) > 0 ? { SourceVPCEs    = var.source_vpce_ids } : {},
      length(var.source_services) > 0 ? { SourceServices = var.source_services } : {}
    )
  )

   
  aoss_principals = distinct(compact([
    aws_iam_role.bedrock_kb_role.arn,
    local.account_root_arn,                                 # <-- add this
    data.aws_caller_identity.current.arn,                   # may be an STS session
    try(data.aws_iam_session_context.current.issuer_arn, null),
  ]))

  kb_index_body = {
    settings = {
      index = {
        knn                               = true
        "knn.algo_param.ef_search"        = tostring(var.index_knn_ef_search)
        number_of_shards                  = "1"
        number_of_replicas                = "1"
      }
    }
    mappings = {
      properties = {
        # Bedrock KB default field names
        "bedrock-knowledge-base-default-vector" = {
          type      = "knn_vector"
          dimension = var.kb_vector_dimension
          method    = {
            engine     = "faiss"
            name       = "hnsw"
            space_type = "l2"
            parameters = { ef_construction = 512, m = 16 }
          }
        }
        "AMAZON_BEDROCK_TEXT_CHUNK" = { type = "text" }
        "AMAZON_BEDROCK_METADATA"   = { type = "text" }
      }
    }
  }
}


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
  description = "Public network policy for KB vector collection"

  policy = jsonencode([{
    Description     = "Public access for KB collection"
    AllowFromPublic = true
    Rules = [
      { ResourceType = "dashboard",  Resource = ["collection/${var.s3_naming_prefix}-kb"] },
      { ResourceType = "collection", Resource = ["collection/${var.s3_naming_prefix}-kb"] }
    ]
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
    Statement = [
      {
      Effect    = "Allow",
      Principal = { Service = "bedrock.amazonaws.com" },
      Action    = "sts:AssumeRole"
      },
      {
      Sid       = "AllowTerraformApplyPrincipal",
      Effect    = "Allow",
      Principal = { AWS =  data.aws_caller_identity.current.arn },
      Action    = "sts:AssumeRole"
      }
    ]
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
        Action   = ["aoss:*"],
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
  description = "Data access policy for Bedrock KB role and Terraform caller"
  policy = jsonencode([{
    Description = "Access to collection and indexes"
    Principal   = local.aoss_principals
    Rules = [
      {
        ResourceType = "collection"
        Resource     = ["collection/*"]
        Permission   = ["aoss:*"]
      },
      {
        ResourceType = "index"
        Resource     = ["index/*/*"]
        Permission   = ["aoss:*"]
      }
    ]
  }])
  lifecycle {
    precondition {
      condition     = length(local.aoss_principals) > 0
      error_message = "AOSS data access policy requires at least one Principal."
    }
  }
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
      vector_index_name =  "bedrock-knowledge-base-default-index" #var.kb_index_name  # <— use the created index
      field_mapping {
        vector_field   = "bedrock-knowledge-base-default-vector"
        text_field     = "AMAZON_BEDROCK_TEXT_CHUNK"
        metadata_field = "AMAZON_BEDROCK_METADATA"
      }
    }
  }

  tags       = var.tags
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
