variable "name_prefix" { type = string }

variable "ecs_cluster_arn"       { type = string }
variable "ecs_task_definition_arn" { type = string }
variable "subnet_ids"            { type = list(string) }
variable "assign_public_ip" { 
 type = bool
 default = false 
 }
 variable aws_region {
  type    = string
  default = "us-east-1"
 }

variable "notification_email" {
  description = "Email address to notify on failure (SNS subscription)."
  type        = string
}

# Optional Scheduler (timezone-aware). If schedule_cron is null, no schedule will be created.
variable "schedule_cron" {
  description = "EventBridge Scheduler cron, e.g. cron(0 6 * * ? *)."
  type        = string
  default     = null
}

variable "schedule_timezone" {
  type    = string
  default = "America/Bogota"
}

variable "tags" {
  type    = map(string)
  default = {}
}

variable vpc_id { type = string }

variable cidr_block {
  description = "CIDR blocks allowed to connect to ECS tasks (usually empty; SFN connects via service role + secret)."
  type        = string
}