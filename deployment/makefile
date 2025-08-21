# === CONFIG ===
.ONESHELL:
SHELL := /bin/bash

IMAGE_NAME = coingecko-ingest
AWS_REGION ?= us-east-1
ACCOUNT_ID := $(shell aws sts get-caller-identity --query "Account" --output text)
REPO_URI   := $(ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(IMAGE_NAME)
TAG        ?= latest
TERRAFORM_DIR ?= .
FUNCTION_NAME = coingecko_snapshot_ingest

KB_INDEX_NAME ?= bedrock-knowledge-base-default-index
KB_VECTOR_DIM ?= 1024

env_file_path = ../env/.env_dev

include ${env_file_path}
export

.PHONY: login buildimage tag push terraform-init terraform-plan \
        terraform-apply terraform-apply-aoss create-aoss-index-boto3 deploy \
        update-lambda clean

# === AUTH ===
login:
	@echo "🔐 Logging in to ECR..."
	@aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $(REPO_URI)

# === DOCKER BUILD & PUSH ===
buildimage:
	@echo "🐳 Building Docker image for Lambda (linux/amd64)..."
	docker build --platform linux/amd64 --provenance=false -t $(IMAGE_NAME) ./lambda_ingest_container

tag:
	@echo "🔖 Tagging image..."
	docker tag $(IMAGE_NAME):latest $(REPO_URI):$(TAG)

push: login buildimage tag
	@echo "🚀 Pushing image to ECR..."
	docker push $(REPO_URI):$(TAG)

# === TERRAFORM ===
terraform-init:
	cd $(TERRAFORM_DIR) && terraform init

terraform-plan:
	@set -a; . $(env_file_path); set +a; \
	cd $(TERRAFORM_DIR) && terraform plan \
	  -var="ingestion_image=$(IMAGE_NAME)"

# Apply only AOSS infra (+ IAM) so index creation can succeed.
terraform-apply-aoss:
	@set -a; . $(env_file_path); set +a; \
	cd $(TERRAFORM_DIR) && terraform apply -auto-approve \
	  -var="ingestion_image=$(IMAGE_NAME)" \
	  -var="kb_index_name=$(KB_INDEX_NAME)" \
	  -target=module.bedrock_kb.aws_opensearchserverless_security_policy.kb_encryption \
	  -target=module.bedrock_kb.aws_opensearchserverless_security_policy.kb_network \
	  -target=module.bedrock_kb.aws_opensearchserverless_collection.kb \
	  -target=module.bedrock_kb.aws_iam_role.bedrock_kb_role \
	  -target=module.bedrock_kb.aws_iam_policy.bedrock_kb_policy \
	  -target=module.bedrock_kb.aws_iam_role_policy_attachment.bedrock_kb_attach \
	  -target=module.bedrock_kb.aws_opensearchserverless_access_policy.kb_data

# Create the index via boto3 (uses collection ARN + optional role assumption).
create-aoss-index-boto3:
	set -euo pipefail
	COL_ARN="$$(terraform -chdir=$(TERRAFORM_DIR) output -raw kb_collection_arn)"
	REG="$(AWS_REGION)"
	IDX="$(KB_INDEX_NAME)"
	ROLE_ARN="$$(terraform -chdir=$(TERRAFORM_DIR) output -raw kb_role_arn)"
	echo "Using collection: $$COL_ARN"
	python3 aoss_create_index.py \
	   --collection-arn "$$COL_ARN" \
	   --region "$$REG" \
	   --index "$$IDX" \
	   --vector-dim "$(KB_VECTOR_DIM)" \
	   --assume-role-arn "$$ROLE_ARN"

# Full apply (now that the index exists, Bedrock KB can be created)
terraform-apply:
	@set -a; . $(env_file_path); set +a; \
	cd $(TERRAFORM_DIR) && terraform apply \
	  -var="ingestion_image=$(IMAGE_NAME)" \
	  -var="kb_index_name=$(KB_INDEX_NAME)" \
	  -auto-approve

deploy: push terraform-init terraform-apply-aoss create-aoss-index-boto3 terraform-apply
	@echo "✅ Deployment complete."

update-lambda:
	aws lambda update-function-code \
		--function-name $(FUNCTION_NAME) \
		--image-uri $(REPO_URI):$(TAG)
	@echo "♻️ Lambda function updated with latest image."

clean:
	docker rmi $(IMAGE_NAME):latest || true
	docker rmi $(REPO_URI):$(TAG) || true
