from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Type

import pandas as pd
from pydantic import BaseModel
from sqlalchemy import create_engine, text

# Ensure project src/ is on sys.path when this module is executed directly.
project_root = Path(__file__).resolve().parents[2]
src_root = project_root / "src"
import sys

if str(src_root) not in sys.path:
    sys.path.append(str(src_root))

from domain.schemas import (
    GungeonGunExternalSchema,
    GungeonItemExternalSchema,
    GungeonGunSynergyExternalSchema,
    GungeonItemSynergyExternalSchema,
)


DEFAULT_DB_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/game_data"


def _get_database_url() -> str:
    """
    Resolve the database URL from env, with a sensible default for docker-compose.
    """
    return os.getenv("DATABASE_URL", DEFAULT_DB_URL)


@dataclass
class LoadPlan:
    """
    Generic plan for loading a Parquet dataset into a Postgres table.
    """

    parquet_path: Path
    table_name: str
    schema: Type[BaseModel]
    truncate_before_load: bool = True


# Load plans for external Gungeon guns, items, and synergies.
LOAD_PLANS: List[LoadPlan] = [
    LoadPlan(
        parquet_path=Path("data/raw/gungeon_guns_external.parquet"),
        table_name="gungeon_guns_external",
        schema=GungeonGunExternalSchema,
        truncate_before_load=True,
    ),
    LoadPlan(
        parquet_path=Path("data/raw/gungeon_items_external.parquet"),
        table_name="gungeon_items_external",
        schema=GungeonItemExternalSchema,
        truncate_before_load=True,
    ),
    LoadPlan(
        parquet_path=Path("data/raw/gungeon_gun_synergies_external.parquet"),
        table_name="gungeon_gun_synergies_external",
        schema=GungeonGunSynergyExternalSchema,
        truncate_before_load=True,
    ),
    LoadPlan(
        parquet_path=Path("data/raw/gungeon_item_synergies_external.parquet"),
        table_name="gungeon_item_synergies_external",
        schema=GungeonItemSynergyExternalSchema,
        truncate_before_load=True,
    ),
]


def load_parquet_plans_to_postgres() -> None:
    """
    Execute all configured LoadPlans:
      - ensure tables exist using the schema DDL
      - optionally TRUNCATE tables (idempotent loads)
      - read Parquet and load rows into each table
    """
    db_url = _get_database_url()
    engine = create_engine(db_url)

    # Ensure tables exist and optionally truncate, in a single transaction.
    with engine.begin() as conn:
        for plan in LOAD_PLANS:
            ddl = plan.schema.CREATE_TABLE_SQL
            conn.execute(text(ddl))
            if plan.truncate_before_load:
                conn.execute(text(f"TRUNCATE TABLE {plan.table_name}"))

    # Load Parquet files and insert rows.
    for plan in LOAD_PLANS:
        if not plan.parquet_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {plan.parquet_path}")

        df = pd.read_parquet(plan.parquet_path)
        if df.empty:
            continue

        # Keep only columns that exist in the schema (plus any extras like 'source').
        schema_fields = set(plan.schema.model_fields.keys())
        cols = [c for c in df.columns if c in schema_fields]
        df_to_load = df[cols]

        df_to_load.to_sql(
            plan.table_name, engine, if_exists="append", index=False, method="multi"
        )


def main() -> None:
    load_parquet_plans_to_postgres()


if __name__ == "__main__":
    main()


