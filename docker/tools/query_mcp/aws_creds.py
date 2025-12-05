# tools/query_mcp/aws_creds.py
import boto3, os

REGION = os.getenv("AWS_REGION", "us-east-1")
IDENTITY_POOL_ID = os.getenv("IDENTITY_POOL_ID")  # TF output
USER_POOL_PROVIDER = os.getenv("USER_POOL_PROVIDER")  # e.g. "cognito-idp.us-east-1.amazonaws.com/us-east-1_AbCdEf123"

def aws_env_from_id_token(id_token: str) -> dict:
    """
    Use Cognito Identity so your Identity Pool role mappings decide which IAM role the user gets.
    Returns an ENV dict you can hand to subprocess.run(env=...).
    """
    idp = boto3.client("cognito-identity", region_name=REGION)
    ident = idp.get_id(IdentityPoolId=IDENTITY_POOL_ID, Logins={USER_POOL_PROVIDER: id_token})
    creds = idp.get_credentials_for_identity(
        IdentityId=ident["IdentityId"],
        Logins={USER_POOL_PROVIDER: id_token},
    )["Credentials"]
    return {
        "AWS_ACCESS_KEY_ID": creds["AccessKeyId"],
        "AWS_SECRET_ACCESS_KEY": creds["SecretKey"],
        "AWS_SESSION_TOKEN": creds["SessionToken"],
        "AWS_REGION": REGION,
    }
