#############################
# Core
#############################

variable "project_prefix" {
  type        = string
  description = "Short prefix used to name resources (e.g., edl, demo)."
}

variable "region" {
  type        = string
  description = "AWS region (e.g., us-east-1)."
}

#############################
# Cognito User Pool
#############################

variable "user_pool_name" {
  type        = string
  description = "Name of the Cognito User Pool."
}

variable "mfa_configuration" {
  type        = string
  default     = "OFF" # OFF | ON | OPTIONAL
  description = "MFA configuration for the User Pool."
  validation {
    condition     = contains(["OFF", "ON", "OPTIONAL"], var.mfa_configuration)
    error_message = "mfa_configuration must be one of: OFF, ON, OPTIONAL."
  }
}

variable "auto_verified_attributes" {
  type        = list(string)
  default     = ["email"]
  description = "Attributes that Cognito will auto-verify (e.g., [\"email\"])."
}

#############################
# App Client (OIDC)
#############################

variable "client_generate_secret" {
  type        = bool
  default     = false
  description = "Whether to generate a client secret for the app client."
}

variable "callback_urls" {
  type        = list(string)
  description = "Allowed OAuth2 redirect (callback) URLs."
}

variable "logout_urls" {
  type        = list(string)
  description = "Allowed OAuth2 logout URLs."
}

variable "allowed_oauth_flows" {
  type        = list(string)
  default     = ["code"]
  description = "Enabled OAuth2 flows (e.g., [\"code\"], [\"implicit\"], [\"client_credentials\"])."
  validation {
    condition = length(setsubtract(var.allowed_oauth_flows, ["code", "implicit", "client_credentials"])) == 0
    error_message = "allowed_oauth_flows can only include: code, implicit, client_credentials."
  }
}

variable "allowed_oauth_scopes" {
  type        = list(string)
  default     = ["openid", "email", "profile"]
  description = "Allowed OAuth2 scopes."
}

variable "supported_identity_providers" {
  type        = list(string)
  default     = ["Okta"]
  description = "List of IdPs enabled for the app client (e.g., [\"COGNITO\"], [\"COGNITO\",\"Google\"])."
}

variable "access_token_validity_minutes" {
  type        = number
  default     = 60
  description = "Access token validity (minutes)."
}

variable "id_token_validity_minutes" {
  type        = number
  default     = 60
  description = "ID token validity (minutes)."
}

variable "refresh_token_validity_days" {
  type        = number
  default     = 30
  description = "Refresh token validity (days)."
}

#############################
# Hosted UI (optional)
#############################

variable "create_hosted_ui_domain" {
  type        = bool
  default     = false
  description = "Whether to create a Cognito Hosted UI domain."
}

variable "hosted_ui_domain_prefix" {
  type        = string
  default     = null  # must be globally unique if set
  description = "Hosted UI domain prefix (global). Required if create_hosted_ui_domain=true."
}

#############################
# Identity Pool
#############################

variable "identity_pool_name" {
  type        = string
  description = "Name of the Cognito Identity Pool."
}

variable "allow_unauthenticated_identities" {
  type        = bool
  default     = false
  description = "Whether unauthenticated identities are allowed."
}

#############################
# Permissions toggles
#############################

variable "enable_bedrock_invoke" {
  type        = bool
  default     = false
  description = "If true, create and attach a Bedrock invoke policy (scoped by bedrock_model_arns)."
}

variable "bedrock_model_arns" {
  type        = list(string)
  default     = []
  description = "List of Bedrock model ARNs to allow (used when enable_bedrock_invoke=true)."
}

variable "attach_basic_logs" {
  type        = bool
  default     = false
  description = "If true, attach a basic CloudWatch Logs policy to the authenticated role."
}

#############################
# Role mappings (Identity Pool Rules)
#############################

variable "role_mapping_rules" {
  description = <<EOT
Explicit role mapping rules (Identity Pool -> Role) evaluated by claim.
Each object: { claim, match_type, value, role_arn }
  - claim: e.g., "cognito:groups", "email", "custom:tenant"
  - match_type: Equals | Contains | StartsWith | NotEqual | etc.
  - value: match value for the claim
  - role_arn: IAM Role ARN to assume when matched
EOT
  type = list(object({
    claim      = string
    match_type = string
    value      = string
    role_arn   = string
  }))
  default = []
}

variable "ambiguous_role_resolution" {
  type        = string
  default     = "AuthenticatedRole" # or "Deny"
  description = "How to resolve ambiguity in role mappings."
  validation {
    condition     = contains(["AuthenticatedRole", "Deny"], var.ambiguous_role_resolution)
    error_message = "ambiguous_role_resolution must be one of: AuthenticatedRole, Deny."
  }
}

#############################
# Optional Admin role controls
#############################

variable "create_admin_role" {
  type        = bool
  default     = false
  description = "If true, create a tenant_admin role."
}

variable "use_strict_admin_trust" {
  type        = bool
  default     = false
  description = "If true, admin role trust also binds to your User Pool provider and requires a tenant session tag."
}

variable "tenant_id" {
  type        = string
  default     = null  # required if use_strict_admin_trust=true
  description = "Tenant ID to require as a session tag when assuming the admin role (strict mode)."
}

variable "admin_group_name" {
  type        = string
  default     = "admins"
  description = "Cognito group name that should map to the tenant_admin role (when add_default_admin_mapping=true)."
}

variable "add_default_admin_mapping" {
  type        = bool
  default     = false
  description = "If true, automatically append a role-mapping rule: group=<admin_group_name> -> tenant_admin role."
}

variable "admin_attach_bedrock" {
  type        = bool
  default     = true
  description = "If true, attach the Bedrock policy to the admin role (when enable_bedrock_invoke=true)."
}

variable "admin_extra_policy_arns" {
  type        = list(string)
  default     = []
  description = "Additional policy ARNs to attach to the admin role."
}

#############################
# Tags
#############################

variable "tags" {
  type        = map(string)
  default     = {}
  description = "Common tags to apply to resources."
}

variable "okta_oidc_issuer" {
  type        = string
  default     = "https://dev-93666616.okta.com/"
}

variable "okta_client_id" {
  type        = string
  default     = "0oaqs8asn1YSHRD0K5d7"
}

variable "okta_client_secret" {
  type        = string
  default     = "9D88zOHI__CB-wAl6Yz5Es4J9-o35Vx8pCJVhW9NBEfyVmKq90qivisu_M9oFOTL"
}