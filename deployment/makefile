# === CONFIG ===
IMAGE_NAME=coingecko-ingest
AWS_REGION=us-east-1
ACCOUNT_ID=$(shell aws sts get-caller-identity --query "Account" --output text)
REPO_URI=$(ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(IMAGE_NAME)
TAG=latest
TERRAFORM_DIR=.
FUNCTION_NAME=coingecko_snapshot_ingest
env_file_path=../env/.env_dev

include ${env_file_path}
export
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

terraform-apply:
	@set -a; . $(env_file_path); set +a; \
	cd $(TERRAFORM_DIR) && terraform apply \
	  -var="ingestion_image=$(IMAGE_NAME)" \
	  -auto-approve

terraform-plan:
	@set -a; . $(env_file_path); set +a; \
	cd $(TERRAFORM_DIR) && terraform plan \
	  -var="ingestion_image=$(IMAGE_NAME)"


deploy: push terraform-init terraform-apply
	@echo "✅ Deployment complete."


update-lambda:
	aws lambda update-function-code \
		--function-name $(FUNCTION_NAME) \
		--image-uri $(REPO_URI):$(TAG)
	@echo "♻️ Lambda function updated with latest image."


clean:
	docker rmi $(IMAGE_NAME):latest || true
	docker rmi $(REPO_URI):$(TAG) || true
