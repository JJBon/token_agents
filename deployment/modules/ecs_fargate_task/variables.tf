variable "name_prefix" { type = string }
variable "vpc_id"             { type = string }
variable "subnet_ids"         { type = list(string) }

variable "ecr_image_url" { type = string }         # e.g. 123456789012.dkr.ecr.us-east-1.amazonaws.com/news-ingest-langgraph
variable "image_tag"     { 
    type = string  
default = "latest" 
}
variable "container_name"{ 
    type = string  
    default = "langgraph-news" 
    }

variable "cpu"    { 
    type = number 
    default = 2048
    }   # 0.5 vCPU
variable "memory" { 
    type = number 
    default = 4096 
    }  # 1 GB

variable "env_vars" {
  description = "Static environment for the container"
  type        = map(string)
  default     = {}
}

variable "secrets_mgr_arns" {
  description = "Optional Secrets Manager ARNs to inject as container secrets (ENV=ARN)."
  type        = map(string)
  default     = {}
}

variable "s3_bucket_arns" {
  description = "Buckets the task may read/write (news KB, athena results, etc.)"
  type        = list(string)
  default     = []
}

variable "extra_task_policy_json" {
  description = "Optional additional IAM policy JSON attached to the task role."
  type        = string
  default     = null
}

variable "enable_execute_command" { 
    type = bool 
    default = false 
    }
