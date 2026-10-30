# TLCN Travel Intelligence Platform

Repository foundation for the Vietnam Travel Intelligence Platform.

## Development

This project uses `uv` for Python environments and dependency management.

```bash
uv sync
uv run ruff check .
uv run pytest
uv run python -c "import travel_intelligence; print(travel_intelligence.__name__)"
```

## G01 lakehouse compatibility spike

TASK-001 adds a minimal MinIO, PostgreSQL JDBC Catalog, Spark/Iceberg, and Trino
integration stack. Copy `.env.example` to `.env`, replace every credential, and
run the complete compatibility gate:

```bash
./scripts/g01/run-g01.sh
```

The runner builds and starts the stack, executes G01-01 through G01-08, restarts
the persistent services without deleting volumes, and writes non-secret evidence
to `tests/e2e/evidence/g01/`. See `docs/technical/compatibility-matrix.md` for the
verified dependency set and `infra/runtime.lock` for the runtime lock manifest.

The Spark History Server is available at `http://localhost:18080`. Spark event
logs are stored in the persistent `spark_events` volume and remain available
after application containers exit. Start it with:

```bash
docker compose --env-file .env -f infra/compose/compose.lakehouse.yaml up -d spark-history-server
```

Spark runs in single-host Standalone mode with one master and one worker. The
master UI is available at `http://localhost:8088`, and the worker UI at
`http://localhost:8089`. Start the cluster and its stable client-mode driver
with:

```bash
docker compose --env-file .env -f infra/compose/compose.lakehouse.yaml \
  up -d spark-master spark-worker spark
```

Submit Python jobs from the stable `spark` driver service so executors can
connect back to its fixed RPC and block-manager ports:

```bash
docker compose --env-file .env -f infra/compose/compose.lakehouse.yaml \
  exec -T spark /opt/entrypoint.sh driver \
  /workspace/scripts/smoke/spark_iceberg_smoke.py verify
```

Application code chooses `SPARK_MASTER_URL` from the environment and falls back
to `local[2]` only outside the Compose Standalone runtime.

Product logic, source ingestion, orchestration, and serving applications remain
out of scope.

## Qdrant infrastructure

The Compose stack includes pinned Qdrant image qdrant/qdrant:v1.14.1, bound to
localhost ports 6333/6334 and persisted in the named qdrant_data volume at
/qdrant/storage. It has no dependency on Spark, Trino, MinIO, or PostgreSQL.

Provision the two TripAdvisor retrieval collections from the embedding and
serving YAML configuration with:

    QDRANT_URL=http://127.0.0.1:6333 +      UV_CACHE_DIR=/tmp/travel-uv-cache uv run python scripts/qdrant/provision.py

The command is idempotent and refuses incompatible vector or payload-index
schemas without deleting or recreating collections. See
docs/tasks/TASK-004B-completion.md for the restart-persistence smoke-test
runbook.

## Configuration and contracts

Root-level enrichment configuration is under configs/:

- configs/enrichment/sentiment/pipeline.yaml and aspect_keywords.yaml define
  preprocessing, segmentation, negation, thresholds, and the preserved
  seven-aspect taxonomy.
- configs/models/visobert.yaml defines the ViSoBERT classification contract.
- configs/models/embedding.yaml intentionally leaves retrieval-model selection
  and vector dimension unconfigured.
- configs/serving/qdrant.yaml defines serving names and environment-based
  connection settings without credentials.

Configuration is loaded and fingerprinted by
travel_intelligence.common.configuration; paths are resolved from the
repository root. TASK-004A defines read-only TripAdvisor Silver eligibility and
proposed sentiment schemas. It does not run inference, create enrichment
tables, generate embeddings, or mutate live Iceberg data.

## Read-only FastAPI foundation

With Trino running on host port 8080, configure the candidate release and start
the API:

```bash
export GOLD_RELEASE_ID=gold-release-9bfd00eeae2748684dbc34e34aab27620f6babed6c8ad8ecfbf8f8fb558c6854
uv run uvicorn travel_intelligence.api.main:app \
  --host 0.0.0.0 --port 8000 --reload
```

Then check liveness, Gold readiness, and OpenAPI documentation:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/ready
```

Open `http://localhost:8000/docs` in a browser. Trino connection, CORS, and
release settings are documented in `.env.example`. The API is read-only and
does not publish or activate the configured candidate release.
