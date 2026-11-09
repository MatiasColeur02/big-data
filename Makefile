# Atajos sobre docker compose. En Windows sin make, usar el comando de la derecha de cada regla.
COMPOSE = docker compose
RUN     = $(COMPOSE) run --rm spark

.DEFAULT_GOAL := help
.PHONY: help up down reset check test shell cqlsh logs

help:  ## Lista los comandos
	@grep -E '^[a-z]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-7s %s\n", $$1, $$2}'

up:  ## Construye la imagen, levanta Cassandra, aplica el esquema y abre Jupyter (localhost:8888)
	$(COMPOSE) up -d --build

check:  ## Verifica versiones y escritura, lectura y TRUNCATE en Cassandra (B2, B3)
	$(RUN) python infra/check_env.py

test:  ## Corre pytest dentro del contenedor
	$(RUN) pytest

shell:  ## Abre una terminal en el contenedor de Spark
	$(RUN) bash

cqlsh:  ## Abre cqlsh contra la Cassandra local
	$(COMPOSE) exec cassandra cqlsh

logs:  ## Sigue los logs de todos los servicios
	$(COMPOSE) logs -f

down:  ## Detiene los contenedores (conserva los datos de Cassandra)
	$(COMPOSE) down

reset:  ## Detiene todo y borra los datos de Cassandra
	$(COMPOSE) down -v
