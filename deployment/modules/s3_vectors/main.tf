terraform {
  required_version = ">= 1.6"
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  bucket_arn = "arn:aws:s3vectors:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:bucket/${var.bucket_name}"
  index_arn  = "${local.bucket_arn}/index/${var.index_name}"

  # Command-line args for the helper (single-line conditionals to please Terraform 1.5.x)
  sse_args      = var.encryption_sse_type == "aws:kms" ? format("--sse-type aws:kms --kms-key-arn %s", var.encryption_kms_key_arn) : "--sse-type AES256"
  meta_keys_arg = length(var.non_filterable_metadata_keys) > 0 ? join(",", var.non_filterable_metadata_keys) : ""

  # Minimal RW policy for app access to this bucket/index
  rw_policy_doc = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid      = "ListDescribe",
        Effect   = "Allow",
        Action   = [
          "s3vectors:GetVectorBucket",
          "s3vectors:ListIndexes",
          "s3vectors:GetIndex"
        ],
        Resource = [
          local.bucket_arn,
          local.index_arn
        ]
      },
      {
        Sid      = "QueryRead",
        Effect   = "Allow",
        Action   = [
          "s3vectors:QueryVectors",
          "s3vectors:GetVectors",
          "s3vectors:ListVectors"
        ],
        Resource = [local.index_arn]
      },
      {
        Sid      = "WriteVectors",
        Effect   = "Allow",
        Action   = [
          "s3vectors:PutVectors",
          "s3vectors:DeleteVectors"
        ],
        Resource = [local.index_arn]
      }
    ]
  })
}
# Create the bucket and index via a small idempotent boto3 helper.
resource "null_resource" "s3vectors_up" {
  triggers = {
    region              = data.aws_region.current.name
    bucket_name         = var.bucket_name
    index_name          = var.index_name
    vector_dimension    = tostring(var.vector_dimension)
    distance_metric     = lower(var.distance_metric)
    encryption_sse_type = var.encryption_sse_type
    encryption_kms_arn  = var.encryption_kms_key_arn
    meta_keys_hash      = sha1(join(",", var.non_filterable_metadata_keys))
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-lc"]
    command = <<-EOT
      set -euo pipefail
      python "${path.module}/scripts/s3vectors_up.py" \
        --region                "${data.aws_region.current.name}" \
        --bucket-name           "${var.bucket_name}" \
        --index-name            "${var.index_name}" \
        --dimension             "${var.vector_dimension}" \
        --distance-metric       "${lower(var.distance_metric)}" \
        --non-filterable-keys   "${local.meta_keys_arg}" \
        ${local.sse_args}
    EOT
  }
}

# Optional: identity-based RW policy you can attach to app roles
resource "aws_iam_policy" "rw" {
  name        = "${var.bucket_name}-${var.index_name}-s3vectors-rw"
  description = "RW access to S3 Vectors index ${var.index_name} in bucket ${var.bucket_name}"
  policy      = local.rw_policy_doc
  tags        = var.tags
}

# Optionally attach the policy to provided role ARNs
resource "aws_iam_role_policy_attachment" "rw_attachments" {
  for_each   = toset(var.attach_to_role_arns)
  role       = each.value
  policy_arn = aws_iam_policy.rw.arn
}
