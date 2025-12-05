# provider_amr already defined earlier:
# locals { provider_amr = "cognito-idp.${var.region}.amazonaws.com/${aws_cognito_user_pool.this.id}" }

data "aws_iam_policy_document" "admins_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals { 
        type = "Federated" 
        identifiers = ["cognito-identity.amazonaws.com"] 
    }
    condition { 
        test = "StringEquals"          
        variable = "cognito-identity.amazonaws.com:aud" 
        values = [aws_cognito_identity_pool.this.id] 
        }
    condition { 
        test = "ForAnyValue:StringLike" 
        variable = "cognito-identity.amazonaws.com:amr" 
        values = ["authenticated"] 
    }
  }
}

data "aws_iam_policy_document" "admins_trust_strict" {
  count = var.use_strict_admin_trust && var.tenant_id != null ? 1 : 0

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals { 
        type = "Federated" 
        identifiers = ["cognito-identity.amazonaws.com"] 
    }
    condition { 
        test = "StringEquals"          
        variable = "cognito-identity.amazonaws.com:aud" 
        values = [aws_cognito_identity_pool.this.id] 
        }
    condition { 
        test = "ForAnyValue:StringLike" 
        variable = "cognito-identity.amazonaws.com:amr" 
        values = ["authenticated", local.provider_amr] 
        }
    condition { 
        test = "StringEquals"           
        variable = "aws:RequestTag/tenant" 
        values = [var.tenant_id] 
        }
    condition { 
        test = "Null"                   
        variable = "aws:RequestTag/tenant" 
        values = ["false"] 
    }
  }
}
