POETRY_RUN = poetry run

DATA_RAW_DIR = data/raw
DB_URL = postgresql+psycopg://postgres:postgres@localhost:5432/game_data

.PHONY: docker-up docker-down docker-logs ingest-raw ingest-gungeon-guns-external db-load db-wipe pipeline-gungeon-guns-external

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

# Fetch Lua data and write raw Parquet snapshots under $(DATA_RAW_DIR)
ingest-raw:
	$(POETRY_RUN) python main.py

# Fetch external Gungeon gun stats (currently via HTML scraper) and write dedicated Parquet
ingest-gungeon-guns-external:
	$(POETRY_RUN) python src/application/writer.py

# Load Parquet snapshots into Postgres according to configured load plans
db-load:
	DATABASE_URL=$(DB_URL) $(POETRY_RUN) python src/application/loader.py

# Wipe current game-related tables in Postgres (idempotent reset)
db-wipe:
	docker compose exec -T db psql -U postgres -d game_data -c "TRUNCATE TABLE gungeon_guns_external;"

# Pipeline for external Gungeon gun stats only
pipeline-gungeon-guns-external: docker-up ingest-gungeon-guns-external db-load


