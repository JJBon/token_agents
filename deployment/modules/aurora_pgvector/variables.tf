variable "name" { type = string }
variable "vpc_id" { type = string }
variable "subnet_ids" { type = list(string) }
variable "db_name" { type = string }

variable "engine_version" {
  type    = string
  default = "16.6"
}
variable "min_acu" {
  type    = number
  default = 0
}
variable "max_acu" {
  type    = number
  default = 1
}
variable "allowed_cidrs" {
  type    = list(string)
  default = []
}

variable "master_username" {
  type        = string
  default     = "kbadmin"   # pick any non-reserved username you like
  description = "Master user for the Aurora cluster (password managed by Secrets Manager)."
}

variable "enable_data_api" {
  type        = bool
  default     = true
  description = "Enable RDS Data API (required for Bedrock KB with RDS storage)."
}