variable "bucket_name" {
  description = "S3 Vector bucket name (region-unique, 3-63 chars, lowercase, digits, hyphens)."
  type        = string
}

variable "index_name" {
  description = "Vector index name (immutable once created)."
  type        = string
}

variable "vector_dimension" {
  description = "Embedding dimensionality (e.g., 1024 for Titan v2)."
  type        = number
  default     = 1024
}

variable "distance_metric" {
  description = "Similarity metric. One of: cosine | euclidean"
  type        = string
  default     = "cosine"
  validation {
    condition     = contains(["cosine", "euclidean"], lower(var.distance_metric))
    error_message = "distance_metric must be cosine or euclidean."
  }
}

variable "non_filterable_metadata_keys" {
  description = "Optional list of metadata keys to treat as non-filterable."
  type        = list(string)
  default     = []
}

variable "encryption_sse_type" {
  description = "SSE type for the vector bucket: AES256 or aws:kms."
  type        = string
  default     = "AES256"
  validation {
    condition     = contains(["AES256", "aws:kms"], var.encryption_sse_type)
    error_message = "encryption_sse_type must be AES256 or aws:kms."
  }
}

variable "encryption_kms_key_arn" {
  description = "KMS CMK ARN (required when encryption_sse_type = aws:kms)."
  type        = string
  default     = ""
}

variable "attach_to_role_arns" {
  description = "Optional list of IAM role ARNs to attach a read/write S3 Vectors policy to."
  type        = list(string)
  default     = []
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "Tags to propagate to IAM policy (for visibility)."
}
