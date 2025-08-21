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

variable "source_vpce_ids" {
  description = "Allowed VPC endpoint IDs for AOSS network policy (optional)"
  type        = list(string)
  default     = []
}

variable "source_services" {
  description = "Allowed AWS services for AOSS network policy (optional)"
  type        = list(string)
  default     = []
}

variable "kb_index_name" {
  type        = string
  description = "Name of the OpenSearch Serverless vector index the KB will use"
  default     = "bedrock-knowledge-base-default-index"
}

variable "kb_vector_dimension" {
  type        = number
  description = "Vector dimension for the index; Titan v2 default is 1024"
  default     = 1024
}

variable "index_knn_ef_search" {
  type        = number
  description = "k-NN ef_search setting (HNSW)"
  default     = 512
}

variable "extra_aoss_principals" {
  description = "Additional IAM principal ARNs allowed on AOSS data policy (users/roles)"
  type        = list(string)
  default     = []
}