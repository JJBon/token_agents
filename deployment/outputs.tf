output "kb_collection_endpoint" {
  value       = module.bedrock_kb.collection_endpoint
  description = "AOSS collection endpoint for index creation"
}

output "kb_role_arn" {
  value       = module.bedrock_kb.role_arn
  description = "KB role ARN for data-plane operations"
}

output "kb_collection_arn" {
  value = module.bedrock_kb.collection_arn # or aws_opensearchserverless_collection.kb.arn
}