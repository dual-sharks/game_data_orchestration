from __future__ import annotations

import os
from pathlib import Path
from typing import List

import pandas as pd
from sqlalchemy import create_engine, text


DEFAULT_DB_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/game_data"


def _get_database_url() -> str:
    """
    Resolve the database URL from env, with a sensible default for docker-compose.
    """
    return os.getenv("DATABASE_URL", DEFAULT_DB_URL)


def _load_parquet_files(raw_dir: Path) -> pd.DataFrame:
    """
    Load all Parquet files under `raw_dir` and concatenate into a single DataFrame.
    """
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw data directory does not exist: {raw_dir}")

    parquet_files: List[Path] = sorted(raw_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found under: {raw_dir}")

    frames = [pd.read_parquet(p) for p in parquet_files]
    return pd.concat(frames, ignore_index=True)


def load_raw_parquet_to_postgres(raw_dir: str | Path = "data/raw") -> None:
    """
    Load all raw Parquet snapshots into a Postgres table.

    - Reads all *.parquet files under `raw_dir`.
    - Ensures a `raw_game_entities` table exists.
    - Appends all rows into that table.
    """
    raw_dir_path = Path(raw_dir)
    df = _load_parquet_files(raw_dir_path)

    db_url = _get_database_url()
    engine = create_engine(db_url)

    # Ensure table exists with a simple, explicit schema.
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS raw_game_entities (
        id BIGSERIAL PRIMARY KEY,
        game_id TEXT NOT NULL,
        game_name TEXT NOT NULL,
        entity_type TEXT NOT NULL,
        external_key TEXT NOT NULL,
        name TEXT,
        quality TEXT,
        raw TEXT NOT NULL
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_table_sql))

    # Only keep the columns we know about, in a stable order.
    cols = ["game_id", "game_name", "entity_type", "external_key", "name", "quality", "raw"]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Expected columns {missing} missing from Parquet data.")

    df = df[cols]

    # Append into the table.
    df.to_sql("raw_game_entities", engine, if_exists="append", index=False, method="multi")


def main() -> None:
    load_raw_parquet_to_postgres()


if __name__ == "__main__":
    main()


