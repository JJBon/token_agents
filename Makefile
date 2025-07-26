.PHONY: run download generate-semantics generate-denorm generate-denorm-all-coins generate-all-coins-metrics dbt-run dbt-docs setup-env install-requirements all

# Define relative paths for directories
DBT_DIR := coindbt
MODELS_DIR := $(DBT_DIR)/models
VENV_DIR := venv
ENVIRONMENT := dev

# Set the dbt profiles directory
export DBT_PROFILES_DIR := $(realpath $(DBT_DIR))

# Transform hyphens to underscores for SQL compatibility
COINS := $(if $(COINS),$(COINS),bitcoin)
SAFE_COIN_LIST := $(shell echo $(COINS) | tr ',' ' ' | sed 's/\.sql//g' | sed 's/-/_/g')

COIN_NAMES := $(shell ls $(MODELS_DIR)/denorm_*_history.sql 2>/dev/null | sed -E "s|.*/denorm_(.*)_history\.sql$$|\1|")
SEMANTIC_TARGETS := $(patsubst %, $(MODELS_DIR)/%_semantic.yml, $(SAFE_COIN_LIST))

setup-env: $(VENV_DIR)/bin/activate

$(VENV_DIR)/bin/activate: requirements.txt
	@echo "Creating virtual environment in $(VENV_DIR)..."
	python3 -m venv $(VENV_DIR)
	@echo "Virtual environment created."
	@echo "Installing dependencies..."
	./$(VENV_DIR)/bin/pip install --upgrade pip
	./$(VENV_DIR)/bin/pip install -r requirements.txt
	touch $(VENV_DIR)/bin/activate  # Ensure this target runs

install-requirements: setup-env

run: setup-env
	./$(VENV_DIR)/bin/python fetch_coin_history.py --coins $(COINS)

generate-semantics: setup-env $(SEMANTIC_TARGETS)
	@echo "All semantic files generated/overwritten."

$(MODELS_DIR)/%_semantic.yml: $(MODELS_DIR)/denorm_%_history.sql semantic_template.yml
	@echo "Generating semantic file for coin: $*"
	mkdir -p $(MODELS_DIR)
	sed 's/{{COIN}}/$*/g' semantic_template.yml > $@
	@echo "Created/overwritten $@ from template."

DENORM_TARGETS := $(patsubst %, $(MODELS_DIR)/denorm_%_history.sql, $(SAFE_COIN_LIST))

# Corrected generate-denorm rule
generate-denorm: setup-env $(DENORM_TARGETS)
	@echo "All per-coin denormalized model files generated/overwritten."

$(MODELS_DIR)/denorm_%_history.sql: denorm_template.sql
	@echo "Generating denorm file for coin: $*"
	mkdir -p $(MODELS_DIR)
	safe_coin=$$(echo "$*" | sed 's/-/_/g'); \
	sed "s/{{COIN}}/$$safe_coin/g" denorm_template.sql > $@
	@echo "Created/overwritten $@ from template."

generate-denorm-all-coins: setup-env $(MODELS_DIR)/denorm_all_coins.sql
	@echo "Master denormalized model for all coins generated/overwritten."

$(MODELS_DIR)/denorm_all_coins.sql: master_coin_denorm.sql
	@echo "Generating master denormalized model from template..."
	mkdir -p $(MODELS_DIR)
	@COIN_UNION=$$(for coin in $(SAFE_COIN_LIST); do \
		echo "select *, '$${coin}' as coin from $${coin}_history"; \
	done | paste -sd " UNION ALL " -); \
	echo "COIN_UNION is: $$COIN_UNION"; \
	sed -e "s|{{COIN_UNION}}|$$COIN_UNION|g" master_coin_denorm.sql > $(MODELS_DIR)/denorm_all_coins.sql; \
	echo "Created/overwritten $(MODELS_DIR)/denorm_all_coins.sql from template."

generate-all-coins-metrics: setup-env $(MODELS_DIR)/all_coins_metrics.yml
	@echo "All coins metrics file generated/overwritten."

$(MODELS_DIR)/all_coins_metrics.yml: render_template.py templates/all_coins_metrics_template.yml
	@echo "Generating all_coins_metrics.yml using render_template.py..."
	./$(VENV_DIR)/bin/python render_template.py

download: setup-env
	@echo "Downloading coin data for coins: $(COINS)..."
	./$(VENV_DIR)/bin/python fetch_coin_history.py --coins $(COINS)

dbt-deps: setup-env
	@echo "Installing dbt package dependencies..."
	@cd $(DBT_DIR) && ../$(VENV_DIR)/bin/dbt deps

dbt-run: dbt-deps
	@echo "Running dbt models..."
	@cd $(DBT_DIR) && \
	if [ -f dbt_project.yml ]; then \
		../$(VENV_DIR)/bin/dbt run --profiles-dir $$DBT_PROFILES_DIR; \
	else \
		echo "Error: No dbt_project.yml found at $(DBT_DIR)/dbt_project.yml"; \
		exit 1; \
	fi

dbt-docs: dbt-deps
	@echo "Generating dbt documentation..."
	@cd $(DBT_DIR) && \
	if [ -f dbt_project.yml ]; then \
		../$(VENV_DIR)/bin/dbt docs generate; \
	else \
		echo "Error: No dbt_project.yml found at $(DBT_DIR)/dbt_project.yml"; \
		exit 1; \
	fi

test_agents: setup-env
	PYTHONPATH=$(CURDIR)/coin_crew pytest coin_crew/tests/db

all: download generate-denorm generate-denorm-all-coins generate-semantics generate-all-coins-metrics dbt-run dbt-docs
	@echo "All tasks including virtual environment setup and installation of requirements completed."


.PHONY: compose-up-spark-dbt
compose-up-spark-dbt:	
	COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose \
	-f ./docker/spark/docker-compose.yml build \
	--build-arg ENVIRONMENT="${environment}" --parallel
	docker-compose -f ./docker/spark/docker-compose.yml up \
	--always-recreate-deps -d

.PHONY: compose-down-spark-dbt
compose-down-spark-dbt:	
	docker-compose -f ./docker/spark/docker-compose.yml down

.PHONY: compose-run-spark-dbt
compose-run-spark-dbt:	compose-up-spark-dbt
	docker-compose -f ./docker/spark/docker-compose.yml exec \
        -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
        -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
		-e AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN}" \
        spark-master \
		/bin/bash -c "/spark_utils/start-thrift-server.sh && /bin/bash"

compose-run-langgraph:
	docker-compose -f ./docker/spark/docker-compose.yml exec \
        -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
        -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
        -e AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN}" \
        langgraph-backend \
        /bin/bash