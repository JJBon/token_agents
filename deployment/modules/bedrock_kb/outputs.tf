output "knowledge_base_id" {
  value       = aws_bedrockagent_knowledge_base.kb.id
  description = "Knowledge Base ID"
}

output "knowledge_base_arn" {
  value       = aws_bedrockagent_knowledge_base.kb.arn
  description = "Knowledge Base ARN"
}

output "data_source_id" {
  value       = aws_bedrockagent_data_source.kb_s3.id
  description = "S3 Data Source ID"
}

output "collection_arn" {
  value       = aws_opensearchserverless_collection.kb.arn
  description = "OpenSearch Serverless collection ARN"
}

output "role_arn" {
  value       = aws_iam_role.bedrock_kb_role.arn
  description = "IAM role used by the KB"
}

output "collection_endpoint" {
  description = "AOSS collection HTTPS endpoint (used to create the index)"
  value       = aws_opensearchserverless_collection.kb.collection_endpoint
}

