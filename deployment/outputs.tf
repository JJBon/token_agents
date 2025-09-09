# Region
output "aws_region" {
  value = var.region
}

# News KB (Aurora/pgvector) bits
output "news_kb_id" {
  value = module.kb_news_pg.kb_id
}

output "research_kb_id" {
  value = module.kb_research_pg.kb_id
}


output "news_kb_data_source_id" {
  value = module.kb_news_pg.data_source_id
}

output  "research_kb_data_source_id" {
  value = module.kb_research_pg.data_source_id
}   


output "news_kb_bucket" {
  value = aws_s3_bucket.news_docs.bucket
}

output "research_kb_bucket" {
  value = aws_s3_bucket.research_docs.bucket
}


# Keep in TF so you can change the prefix in one place
output "news_kb_prefix" {
  value = "news/"
}

output "aurora_cluster_arn" {
    value = module.aurora_pg.cluster_arn
}

output "aurora_secret_arn" {
    value = module.aurora_pg.secret_arn
}