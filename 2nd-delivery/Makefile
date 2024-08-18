## 
## ----------------------------------------------------------------------
## Welcome to the 2nd delivery of the final project. Coderhouse's Data Engineering
## Student: Orengia Christian
## Commission: 61890
## ----------------------------------------------------------------------
## With this delivery you will
## - extract data from Population.io and Open-Meteo
## - transform it to be used in the study of the relation between the
## wheather and the population movements around the world
## - Load it to a data warehouse in redshift
## ----------------------------------------------------------------------
## 
## Available command list:
## 

# Precarga de constantes y variables de entorno

COMPOSE_PROJECT_NAME=2nd-delivery-final-project

DOCKER_PATH=./docker-compose.yml
DATABASE_SCRIPT=/data/db-setup/setup.sql
PYTHON_SCRIPT=/scripts/etl.py

# Cargar variables de entorno desde credentials.env y settings.env
ifneq (,$(wildcard credentials.env))
    include credentials.env
    export $(shell sed 's/=.*//' credentials.env)
endif

ifneq (,$(wildcard settings.env))
    include settings.env
    export $(shell sed 's/=.*//' settings.env)
endif

help: ## show this help.
	@sed -ne '/@sed/!s/## //p' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ": "}; {printf "\033[1;32m%-30s\033[0m %s\n", $$1, $$2}'

all: ## Run full delivery.
					## 1. Show project details
					## 2. Reset and build containers
					## 3. Extract, transform and load APIs data
					## 4. Open Redshift shell
	@$(MAKE) show-project-details
	@$(MAKE) build-reset
	@$(MAKE) etl
	@$(MAKE) psql

show-project-details: ## show the project details
	@echo ""
	@echo "	\033[1;32mContainer name:\033[0m		" $(COMPOSE_PROJECT_NAME)
	@echo "	\033[1;32mDatabase service name:\033[0m	" $(DB_SERVICE_NAME)
	@echo "	\033[1;32mDatabase host:\033[0m		" $(DB_HOST)
	@echo "	\033[1;32mDatabase port:\033[0m		" $(DB_PORT)
	@echo "	\033[1;32mDatabase name:\033[0m		" $(DB_DATABASE)
	@echo "	\033[1;32mDatabase username:\033[0m	" $(DB_USERNAME)
	@echo "	\033[1;32mDatabase password:\033[0m	 This is your secret..."
	@echo ""

build: ## build the solution
	@echo "Starting container and importing database"
	docker-compose -f $(DOCKER_PATH) up -d --build
	@echo "Waiting for the database to be ready..."
	@while ! docker exec $(DB_SERVICE_NAME) pg_isready -U postgres; do sleep 1; done
	@echo "Running script.sql inside the container"
	docker exec -it $(PYTHON_CONTAINER) bash -c "export PGPASSWORD=$(DB_PWD) && psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USERNAME) -d $(DB_DATABASE) -f $(DATABASE_SCRIPT)"

build-reset: ## build the solution with a container reset
	@echo "Restarting solution"
	docker compose -f $(DOCKER_PATH) down
	@echo "Starting container and importing database"
	docker-compose -f $(DOCKER_PATH) up -d --build
	@echo "Waiting for the database to be ready..."
	@while ! docker exec $(DB_SERVICE_NAME) pg_isready -U postgres; do sleep 1; done
	@echo "Running script.sql inside the container"
	docker exec -it $(PYTHON_CONTAINER) bash -c "export PGPASSWORD=$(DB_PWD) && psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USERNAME) -d $(DB_DATABASE) -f $(DATABASE_SCRIPT)"

psql: ## Open psql console
	@echo "\033[1;32mOpening psql console in redshift container\033[0m"
	docker exec -it $(PYTHON_CONTAINER) bash -c "export PGPASSWORD=$(DB_PWD) && psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USERNAME) -d $(DB_DATABASE)"

etl: ## Extract, transform and load data from APIs with python scripts
	@echo ""
	@echo "\033[1;34mRunning Python script...\033[0m"
	docker exec -it $(PYTHON_CONTAINER) python $(PYTHON_SCRIPT)
	@echo ""

down: ## Clean all project
	@echo "Clean all project"
	docker compose -f $(DOCKER_PATH) down

.PHONY: show-project-details all build build-reset psql down