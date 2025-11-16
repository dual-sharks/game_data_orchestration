POETRY_RUN = poetry run

DATA_RAW_DIR = data/raw
DB_URL = postgresql+psycopg://postgres:postgres@localhost:5432/game_data

.PHONY: docker-up docker-down docker-logs ingest-raw db-load-raw pipeline

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

# Fetch Lua data and write raw Parquet snapshots under $(DATA_RAW_DIR)
ingest-raw:
	$(POETRY_RUN) python main.py

# Load raw Parquet snapshots into Postgres (raw_game_entities table)
db-load-raw:
	DATABASE_URL=$(DB_URL) $(POETRY_RUN) python src/load_to_postgres.py

# Full pipeline: start DB, ingest, load into Postgres
pipeline: docker-up ingest-raw db-load-raw


