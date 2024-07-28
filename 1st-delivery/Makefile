## 
## ----------------------------------------------------------------------
## Welcome to the 1st delivery of the final project. Coderhouse's Data Engineering
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

COMPOSE_PROJECT_NAME=1st-delivery-final-project

DOCKER_PATH=./docker-compose.yml
DATABASE_SCRIPT=/data/db-setup/setup.sql

# Cargar variables de entorno desde credentials.env
ifneq (,$(wildcard credentials.env))
    include credentials.env
    export $(shell sed 's/=.*//' credentials.env)
endif

help: ## show this help.
	@sed -ne '/@sed/!s/## //p' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ": "}; {printf "\033[1;32m%-30s\033[0m %s\n", $$1, $$2}'

all: ## Run full delivery
	@$(MAKE) show-project-details
	@$(MAKE) build-reset
	@$(MAKE) psql

show-project-details: ## show the project details
	@echo ""
	@echo "	\033[1;32mContainer name:\033[0m		" $(COMPOSE_PROJECT_NAME)
	@echo "	\033[1;32mPostgress service name:\033[0m	" $(PG_SERVICE_NAME)
	@echo "	\033[1;32mPostgress port:\033[0m		" $(PG_PORT)
	@echo "	\033[1;32mPostgress password:\033[0m	" $(PG_PWD)
	@echo ""

build: ## build the solution
	@echo "Starting container and importing database"
	docker-compose -f $(DOCKER_PATH) up -d --build
	@echo "Waiting for the database to be ready..."
	@while ! docker exec $(PG_SERVICE_NAME) pg_isready -U postgres; do sleep 1; done
	@echo "Running script.sql inside the container"
	docker exec -it $(PG_SERVICE_NAME) psql -h db -p $(PG_PORT) -U postgres -f $(DATABASE_SCRIPT)

build-reset: ## build the solution with a container reset
	@echo "Restarting solution"
	docker compose -f $(DOCKER_PATH) down
	@echo "Starting container and importing database"
	docker-compose -f $(DOCKER_PATH) up -d --build
	@echo "Waiting for the database to be ready..."
	@while ! docker exec $(PG_SERVICE_NAME) pg_isready -U postgres; do sleep 1; done
	@echo "Running script.sql inside the container"
	docker exec -it $(PG_SERVICE_NAME) psql -h db -p $(PG_PORT) -U postgres -f $(DATABASE_SCRIPT)

psql: ## Open psql console
	@echo "Opening psql console in redshift container"
	docker exec -it $(PG_SERVICE_NAME) psql -h db -p $(PG_PORT) -U postgres

down: ## Clean all project
	@echo "Clean all project"
	docker compose -f $(DOCKER_PATH) down

## 

# all: up psql

# up:
# 	@echo "Starting redshift container and importing database (if configured)"
# 	docker-compose -f $(DOCKER_PATH) up -d --build
# 	@echo "Running script.sql inside the container"
# 	docker exec -it $(PG_SERVICE_NAME) psql -h db -p $(PG_PORT) -U postgres -f $(DATABASE_SCRIPT)

# psql:
# 	@echo "Opening psql console in redshift container"
# 	docker exec -it $(PG_SERVICE_NAME) psql -h db -p $(PG_PORT) -U postgres

# down:
# 	@echo "Clean all project"
# 	docker compose -f $(DOCKER_PATH) down

# clean:
# 	@echo "Delete all"
# 	docker system prune -a
# 	@echo "Bye"

.PHONY: show-project-details all build build-reset psql down clean