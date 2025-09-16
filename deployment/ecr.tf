resource "aws_ecr_repository" "news_ingest_langgraph" {
  name                 = "news-ingest-langgraph"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = false
  }
}