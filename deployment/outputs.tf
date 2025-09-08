# Region
output "aws_region" {
  value = var.region
}

# News KB (Aurora/pgvector) bits
output "news_kb_id" {
  value = module.kb_news_pg.kb_id
}

output "news_kb_data_source_id" {
  value = module.kb_news_pg.data_source_id
}

output "news_kb_bucket" {
  value = aws_s3_bucket.news_docs.bucket
}

# Keep in TF so you can change the prefix in one place
output "news_kb_prefix" {
  value = "news/"
}
