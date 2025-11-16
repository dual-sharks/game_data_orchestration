from __future__ import annotations

import os
from pathlib import Path
from typing import List

import pandas as pd
from sqlalchemy import create_engine, text

from application.gungeon_curation import GungeonCurationService
from domain.schemas import (
    RawGameEntitySchema,
    GungeonGunSchema,
    GungeonGunStatSchema,
)


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
    Load all raw Parquet snapshots into Postgres.

    - Reads all *.parquet files under `raw_dir`.
    - Ensures a `raw_game_entities` table exists and appends all rows.
    - Delegates to application-level curation to build Gungeon-specific tables.
    """
    raw_dir_path = Path(raw_dir)
    df = _load_parquet_files(raw_dir_path)

    db_url = _get_database_url()
    engine = create_engine(db_url)

    # Ensure raw table exists with a simple, explicit schema.
    create_raw_sql = RawGameEntitySchema.CREATE_TABLE_SQL

    # Ensure curated tables exist for Gungeon guns and their stats.
    create_guns_sql = GungeonGunSchema.CREATE_TABLE_SQL

    create_gun_stats_sql = GungeonGunStatSchema.CREATE_TABLE_SQL

    with engine.begin() as conn:
        conn.execute(text(create_raw_sql))
        conn.execute(text(create_guns_sql))
        conn.execute(text(create_gun_stats_sql))

    # Only keep the columns we know about, in a stable order.
    cols = ["game_id", "game_name", "entity_type", "external_key", "name", "quality", "raw"]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Expected columns {missing} missing from Parquet data.")

    df = df[cols]

    # Append into the raw table.
    df.to_sql("raw_game_entities", engine, if_exists="append", index=False, method="multi")

    # Application-level curation for Gungeon.
    service = GungeonCurationService()

    curated_guns = service.curate_guns(df)
    if not curated_guns.empty:
        curated_guns.to_sql("gungeon_guns", engine, if_exists="append", index=False, method="multi")

    curated_stats = service.curate_gun_stats(df)
    if not curated_stats.empty:
        curated_stats.to_sql("gungeon_gun_stats", engine, if_exists="append", index=False, method="multi")


def main() -> None:
    load_raw_parquet_to_postgres()


if __name__ == "__main__":
    main()
