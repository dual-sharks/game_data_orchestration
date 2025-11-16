from __future__ import annotations

from typing import ClassVar, Optional

from pydantic import BaseModel


class RawGameEntitySchema(BaseModel):
    """
    Logical schema for the `raw_game_entities` table.

    We keep the SQL DDL here so the loader can stay generic and the
    table definition lives alongside a typed view of the row.
    """

    game_id: str
    game_name: str
    entity_type: str
    external_key: str
    name: Optional[str] = None
    quality: Optional[str] = None
    raw: str

    CREATE_TABLE_SQL: ClassVar[str] = """
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


class GungeonGunSchema(BaseModel):
    """
    Logical schema for the curated `gungeon_guns` table.
    """

    game_id: str
    gun_key: str
    gun_name: Optional[str] = None
    rarity: Optional[str] = None
    quote: Optional[str] = None
    description: Optional[str] = None
    categories: Optional[str] = None
    unlock: Optional[str] = None
    corrupt: Optional[str] = None
    gun_numeric_id: Optional[int] = None
    localization_internal_name: Optional[str] = None
    stats: Optional[str] = None

    CREATE_TABLE_SQL: ClassVar[str] = """
    CREATE TABLE IF NOT EXISTS gungeon_guns (
        id BIGSERIAL PRIMARY KEY,
        game_id TEXT NOT NULL,
        gun_key TEXT NOT NULL,
        gun_name TEXT,
        rarity TEXT,
        quote TEXT,
        description TEXT,
        categories TEXT,
        unlock TEXT,
        corrupt TEXT,
        gun_numeric_id INTEGER,
        localization_internal_name TEXT,
        stats TEXT
    );
    """


class GungeonGunStatSchema(BaseModel):
    """
    Logical schema for the curated `gungeon_gun_stats` table.
    """

    game_id: str
    gun_key: str
    gun_name: Optional[str] = None
    idx: Optional[int] = None
    stat_name: Optional[str] = None
    value_raw: Optional[str] = None
    value_numeric: Optional[float] = None
    stack_mode: Optional[str] = None
    add_raw: Optional[str] = None
    add_numeric: Optional[float] = None

    CREATE_TABLE_SQL: ClassVar[str] = """
    CREATE TABLE IF NOT EXISTS gungeon_gun_stats (
        id BIGSERIAL PRIMARY KEY,
        game_id TEXT NOT NULL,
        gun_key TEXT NOT NULL,
        gun_name TEXT,
        idx INTEGER,
        stat_name TEXT,
        value_raw TEXT,
        value_numeric DOUBLE PRECISION,
        stack_mode TEXT,
        add_raw TEXT,
        add_numeric DOUBLE PRECISION
    );
    """


