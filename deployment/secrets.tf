resource "aws_secretsmanager_secret" "cryptonews_token" {
  name        = "cryptonews/api_token"
  description = "API token for cryptonews-api.com"
}