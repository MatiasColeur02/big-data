# Atajos sobre docker compose. En Windows sin make, usar el comando de la derecha de cada regla.
#
# PROFILE=cloud corre cualquier regla contra AstraDB (D17): toma ASTRA_DB_TOKEN del .env y el bundle
# de la raíz del repo. Ej.: `make pipeline PROFILE=cloud`, `make idempotency PROFILE=cloud`.
COMPOSE = docker compose
RUN     = $(COMPOSE) run --rm $(if $(PROFILE),-e PIPELINE_PROFILE=$(PROFILE),) spark

.DEFAULT_GOAL := help
.PHONY: help up down reset check check-astra colab-emu idempotency test pipeline clean bronze quality silver gold serving queries shell cqlsh logs

help:  ## Lista los comandos
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-12s %s\n", $$1, $$2}'

up:  ## Construye la imagen, levanta Cassandra, aplica el esquema y abre Jupyter (localhost:8888)
	$(COMPOSE) up -d --build

idempotency:  ## Prueba de idempotencia O6 (3 corridas); PROFILE=cloud para AstraDB
	$(RUN) python -m src.idempotency

check-astra:  ## Verifica escritura, lectura y TRUNCATE en AstraDB; salida en evidence/entrega2/cloud (2.9)
	@mkdir -p evidence/entrega2/cloud
	set -o pipefail; $(RUN) python infra/check_astra.py | grep -v '^:: loading' | tee evidence/entrega2/cloud/check_astra.txt

colab-emu:  ## Corre 02_pipeline.ipynb en un emulador de Colab (Python 3.12) contra AstraDB; evidencia en evidence/entrega2/cloud (D22)
	docker build -q -t cloud-analytics-colab-emu infra/colab
	docker run --rm --env-file .env \
		-e PIPELINE_PROFILE=cloud -e PYTHONPATH=/content/big-data \
		-e ASTRA_BUNDLE_PATH=/content/secure-connect-cloud-analytics.zip \
		-v "$(CURDIR)":/repo:ro \
		-v "$(CURDIR)/secure-connect-cloud-analytics.zip":/content/secure-connect-cloud-analytics.zip:ro \
		-v "$(CURDIR)/evidence/entrega2/cloud":/out \
		-v colab-emu-ivy:/root/.ivy2 \
		cloud-analytics-colab-emu bash /repo/infra/colab/run.sh

check:  ## Verifica versiones y escritura, lectura y TRUNCATE en Cassandra (B2, B3)
	$(RUN) python infra/check_env.py

test:  ## Corre pytest dentro del contenedor
	$(RUN) pytest

pipeline:  ## Corre el pipeline completo: Bronze → calidad → Silver → Gold → Serving (2.8)
	$(RUN) python -m src.pipeline

clean:  ## Borra juntas las zonas generadas del lake y los checkpoints (D13); Landing no se toca
	$(RUN) python -m src.pipeline --clean-only

bronze:  ## Corre Bronze: maestros batch y stream de eventos
	$(RUN) python -m src.ingest.batch_masters
	$(RUN) python -m src.ingest.stream_events

quality:  ## Reglas de calidad sobre Bronze y sandbox de fixtures (D20); evidencia en evidence/entrega2/
	$(RUN) python -m src.quality.events
	$(RUN) python -m src.quality.sandbox

silver:  ## Corre Silver: dimensiones, calidad, eventos conformados y features diarias
	$(RUN) python -m src.silver.run

gold:  ## Corre Gold: mart diario, Top-N 14 días y anomalías MAD; evidencia en evidence/entrega2/gold
	$(RUN) python -m src.gold.run

serving:  ## Carga Gold en Cassandra (upsert + TRUNCATE del Top-N) y corre P1/P2; evidencia en evidence/entrega2/serving
	$(RUN) python -m src.serving.load_cassandra

queries:  ## Corre 03_queries.cql con cqlsh y con el driver; salida en evidence/entrega2/serving
	@mkdir -p evidence/entrega2/serving
	$(COMPOSE) run --rm --no-deps -T --entrypoint cqlsh cassandra-init cassandra 9042 -f /cql/03_queries.cql \
		> evidence/entrega2/serving/cqlsh_queries.txt
	$(RUN) python -m src.serving.queries

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
