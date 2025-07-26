from langchain_aws import ChatBedrockConverse
import boto3


bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
query_llm = ChatBedrockConverse(
    model="anthropic.claude-3-haiku-20240307-v1:0",
    provider="anthropic",
    temperature=0,
    client=bedrock,
    disable_streaming="tool_calling"
)