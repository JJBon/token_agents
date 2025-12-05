# ---------------------------
# Core locals
# ---------------------------
locals {
  user_pool_provider_url = "cognito-idp.${var.region}.amazonaws.com/${aws_cognito_user_pool.this.id}"
  tags                   = merge({ "terraform-module" = "cognito-auth" }, var.tags)
  identity_pool_role_mapping_provider = "${local.user_pool_provider_url}:${aws_cognito_user_pool_client.this.id}"
}

# ---------------------------
# Cognito User Pool
# ---------------------------
resource "aws_cognito_user_pool" "this" {
  name                       = var.user_pool_name
  mfa_configuration          = var.mfa_configuration
  auto_verified_attributes   = var.auto_verified_attributes

  password_policy {
    minimum_length    = 12
    require_lowercase = true
    require_numbers   = true
    require_symbols   = true
    require_uppercase = true
  }

  account_recovery_setting {
    recovery_mechanism {
      name     = "verified_email"
      priority = 1
    }
  }

  tags = local.tags
}

# ---------------------------
# App Client (OIDC)
# ---------------------------
resource "aws_cognito_user_pool_client" "this" {
  name                                 = "${var.project_prefix}-app-client"
  user_pool_id                         = aws_cognito_user_pool.this.id
  generate_secret                      = var.client_generate_secret

  callback_urls                        = var.callback_urls
  logout_urls                          = var.logout_urls

  allowed_oauth_flows                  = var.allowed_oauth_flows
  allowed_oauth_scopes                 = var.allowed_oauth_scopes
  allowed_oauth_flows_user_pool_client = true
  supported_identity_providers         = var.supported_identity_providers

  explicit_auth_flows = [
    "ALLOW_REFRESH_TOKEN_AUTH",
    "ALLOW_USER_SRP_AUTH",
    "ALLOW_USER_PASSWORD_AUTH"
  ]

  enable_token_revocation       = true
  prevent_user_existence_errors = "ENABLED"

  access_token_validity  = var.access_token_validity_minutes
  id_token_validity      = var.id_token_validity_minutes
  refresh_token_validity = var.refresh_token_validity_days

  token_validity_units {
    access_token  = "minutes"
    id_token      = "minutes"
    refresh_token = "days"
  }

}

# ---------------------------
# Hosted UI Domain (optional)
# ---------------------------
resource "aws_cognito_user_pool_domain" "this" {
  count        = var.create_hosted_ui_domain ? 1 : 0
  domain       = var.hosted_ui_domain_prefix
  user_pool_id = aws_cognito_user_pool.this.id
}

# ---------------------------
# Identity Pool
# ---------------------------
resource "aws_cognito_identity_pool" "this" {
  identity_pool_name               = var.identity_pool_name
  allow_unauthenticated_identities = var.allow_unauthenticated_identities

  cognito_identity_providers {
    client_id               = aws_cognito_user_pool_client.this.id
    provider_name           = local.user_pool_provider_url
    server_side_token_check = false
  }

  tags = local.tags
}

# ---------------------------
# Trust policies (Auth / Unauth)
# ---------------------------
data "aws_iam_policy_document" "auth_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = ["cognito-identity.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "cognito-identity.amazonaws.com:aud"
      values   = [aws_cognito_identity_pool.this.id]
    }
    condition {
      test     = "ForAnyValue:StringLike"
      variable = "cognito-identity.amazonaws.com:amr"
      values   = ["authenticated"]
    }
  }
}

data "aws_iam_policy_document" "unauth_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = ["cognito-identity.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "cognito-identity.amazonaws.com:aud"
      values   = [aws_cognito_identity_pool.this.id]
    }
    condition {
      test     = "ForAnyValue:StringLike"
      variable = "cognito-identity.amazonaws.com:amr"
      values   = ["unauthenticated"]
    }
  }
}

# ---------------------------
# Default roles (Auth / Unauth)
# ---------------------------
resource "aws_iam_role" "authenticated" {
  name               = "${var.project_prefix}-cognito-authenticated"
  assume_role_policy = data.aws_iam_policy_document.auth_assume.json
  tags               = local.tags
}

resource "aws_iam_role" "unauthenticated" {
  name               = "${var.project_prefix}-cognito-unauthenticated"
  assume_role_policy = data.aws_iam_policy_document.unauth_assume.json
  tags               = local.tags
}

# Optional: Bedrock invoke policy and attachment to authenticated role
resource "aws_iam_policy" "bedrock_invoke" {
  count  = var.enable_bedrock_invoke ? 1 : 0
  name   = "${var.project_prefix}-bedrock-invoke"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Sid    = "InvokeSpecificModels",
      Effect = "Allow",
      Action = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
      Resource = var.bedrock_model_arns
    }]
  })
}

resource "aws_iam_role_policy_attachment" "auth_bedrock" {
  count      = var.enable_bedrock_invoke ? 1 : 0
  role       = aws_iam_role.authenticated.name
  policy_arn = aws_iam_policy.bedrock_invoke[0].arn
}

# Optional: basic CloudWatch logs on authenticated role
resource "aws_iam_policy" "basic_logs" {
  count  = var.attach_basic_logs ? 1 : 0
  name   = "${var.project_prefix}-cognito-basic-logs"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "auth_logs" {
  count      = var.attach_basic_logs ? 1 : 0
  role       = aws_iam_role.authenticated.name
  policy_arn = aws_iam_policy.basic_logs[0].arn
}

# ---------------------------
# Optional Admin role (counted)
# ---------------------------
# Strict trust variant needs a tenant tag + binds to this user pool AMR
locals {
  provider_amr = "cognito-idp.${var.region}.amazonaws.com/${aws_cognito_user_pool.this.id}"
}


resource "aws_iam_role" "tenant_admin" {
  count = var.create_admin_role ? 1 : 0

  name = "${var.project_prefix}-tenant-admin"

  assume_role_policy = (
    var.use_strict_admin_trust && var.tenant_id != null
    ? data.aws_iam_policy_document.admins_trust_strict[0].json
    : data.aws_iam_policy_document.admins_trust.json
  )

  tags = local.tags
}

# Optional: attach Bedrock policy and any extras to admin role
resource "aws_iam_role_policy_attachment" "admin_bedrock" {
  count      = var.create_admin_role && var.enable_bedrock_invoke && var.admin_attach_bedrock ? 1 : 0
  role       = aws_iam_role.tenant_admin[0].name
  policy_arn = aws_iam_policy.bedrock_invoke[0].arn
}

resource "aws_iam_role_policy_attachment" "admin_extra_policies" {
  count      = var.create_admin_role ? length(var.admin_extra_policy_arns) : 0
  role       = aws_iam_role.tenant_admin[0].name
  policy_arn = var.admin_extra_policy_arns[count.index]
}

# ---------------------------
# Safe locals that depend on admin role
# ---------------------------
locals {
  tenant_admin_arn = var.create_admin_role && length(aws_iam_role.tenant_admin) > 0 ? aws_iam_role.tenant_admin[0].arn : null

  default_admin_rule = var.add_default_admin_mapping && local.tenant_admin_arn != null ? [
    {
      claim      = "cognito:groups"
      match_type = "Contains"
      value      = var.admin_group_name
      role_arn   = local.tenant_admin_arn
    }
  ] : []

  combined_role_mapping_rules = concat(var.role_mapping_rules, local.default_admin_rule)
}

# ---------------------------
# Attach default roles to Identity Pool + RoleMappings
# ---------------------------
resource "aws_cognito_identity_pool_roles_attachment" "roles" {
  identity_pool_id = aws_cognito_identity_pool.this.id

  roles = {
    authenticated   = aws_iam_role.authenticated.arn
    unauthenticated = aws_iam_role.unauthenticated.arn
  }

  # NOTE: block name is singular: role_mapping
  dynamic "role_mapping" {
    for_each = length(local.combined_role_mapping_rules) > 0 ? [1] : []
    content {
      identity_provider         = local.identity_pool_role_mapping_provider
      type                      = "Rules"
      ambiguous_role_resolution = var.ambiguous_role_resolution

      # Repeatable "mapping_rule" blocks (NOT rules_configuration)
      dynamic "mapping_rule" {
        for_each = local.combined_role_mapping_rules
        content {
          claim      = mapping_rule.value.claim
          match_type = mapping_rule.value.match_type   # Equals | Contains | StartsWith | NotEqual | etc.
          value      = mapping_rule.value.value
          role_arn   = mapping_rule.value.role_arn
        }
      }
    }
  }
}

resource "aws_cognito_identity_provider" "okta_oidc" {
  user_pool_id  = aws_cognito_user_pool.this.id
  provider_name = "Okta"
  provider_type = "OIDC"

  provider_details = {
    oidc_issuer               = var.okta_oidc_issuer         
    client_id                 = var.okta_client_id
    client_secret             = var.okta_client_secret
    authorize_scopes          = "openid profile email"
    attributes_request_method = "GET"
  }

  attribute_mapping = {
    email       = "email"
    given_name  = "given_name"
    family_name = "family_name"
    # optional custom claim mapping:
    # "custom:tenant" = "tenant"
  }
}