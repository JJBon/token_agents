output "user_pool_id" { value = aws_cognito_user_pool.this.id }
output "user_pool_arn" { value = aws_cognito_user_pool.this.arn }
output "user_pool_client_id" { value = aws_cognito_user_pool_client.this.id }
output "user_pool_domain" { value = try(aws_cognito_user_pool_domain.this[0].domain, null) }


output "issuer" { value = "https://${local.user_pool_provider_url}" }
output "jwks_url" { value = "https://${local.user_pool_provider_url}/.well-known/jwks.json" }


output "identity_pool_id" { value = aws_cognito_identity_pool.this.id }


output "authenticated_role_arn" { value = aws_iam_role.authenticated.arn }
output "unauthenticated_role_arn" { value = aws_iam_role.unauthenticated.arn }
output "tenant_admin_role_arn" { value = try(aws_iam_role.tenant_admin[0].arn, null) }

