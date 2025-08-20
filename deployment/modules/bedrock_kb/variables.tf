variable "kb_name" {
  type        = string
  description = "Bedrock Knowledge Base name"
}

variable "s3_naming_prefix" {
  type        = string
  description = "Prefix used to name AOSS collection/policies"
}

variable "s3_bucket_arn" {
  type        = string
  description = "ARN of the S3 bucket that stores documents"
}

variable "s3_inclusion_prefixes" {
  type        = list(string)
  description = "List of S3 prefixes with documents to ingest"
  default     = ["kb-docs/"]
}

variable "kb_embedding_model_arn" {
  type        = string
  description = "Embedding model ARN for the knowledge base"
  default     = "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v2:0"
}

variable "allow_public_network" {
  type        = bool
  description = "If true, allow public network access to the AOSS collection (quick start)"
  default     = true
}

variable "tags" {
  type        = map(string)
  description = "Tags to apply to created resources"
  default     = {}
}
