
variable "region" {
  type        = string
  default     = "us-east-1"
  description = "description"
}


variable "s3_naming_prefix" {
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

variable "cryptonews_url" {
  type = string
  default = "https://cryptonews-api.com/api/v1/category?section=general&items=50&page=2"
}

variable "alert_email" {
  type        = string
  description = "Email address to send alerts to (via SNS)"
  default     = ""
}