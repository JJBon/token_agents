
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


variable "kb_index_name" {
  type        = string
  default     = "bedrock-knowledge-base-default-index"
  description = "AOSS vector index name for the KB"
}

variable "kb_vector_dimension" {
  type        = number
  default     = 1024
  description = "Vector dimension (Titan v2 default = 1024)"
}

