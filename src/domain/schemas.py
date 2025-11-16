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


class GungeonItemSchema(BaseModel):
    """
    Logical schema for the curated `gungeon_items` table.
    """

    game_id: str
    item_key: str
    item_name: Optional[str] = None
    rarity: Optional[str] = None
    quote: Optional[str] = None
    description: Optional[str] = None
    categories: Optional[str] = None
    unlock: Optional[str] = None
    corrupt: Optional[str] = None
    item_numeric_id: Optional[int] = None
    localization_internal_name: Optional[str] = None
    stats: Optional[str] = None

    CREATE_TABLE_SQL: ClassVar[str] = """
    CREATE TABLE IF NOT EXISTS gungeon_items (
        id BIGSERIAL PRIMARY KEY,
        game_id TEXT NOT NULL,
        item_key TEXT NOT NULL,
        item_name TEXT,
        rarity TEXT,
        quote TEXT,
        description TEXT,
        categories TEXT,
        unlock TEXT,
        corrupt TEXT,
        item_numeric_id INTEGER,
        localization_internal_name TEXT,
        stats TEXT
    );
    """


class RiskOfRain2ItemSchema(BaseModel):
    """
    Logical schema for the curated `riskofrain2_items` table.
    """

    game_id: str
    item_key: str
    item_name: Optional[str] = None
    rarity: Optional[str] = None
    quote: Optional[str] = None
    description: Optional[str] = None
    categories: Optional[str] = None
    unlock: Optional[str] = None
    item_numeric_id: Optional[int] = None
    localization_internal_name: Optional[str] = None
    stats: Optional[str] = None

    CREATE_TABLE_SQL: ClassVar[str] = """
    CREATE TABLE IF NOT EXISTS riskofrain2_items (
        id BIGSERIAL PRIMARY KEY,
        game_id TEXT NOT NULL,
        item_key TEXT NOT NULL,
        item_name TEXT,
        rarity TEXT,
        quote TEXT,
        description TEXT,
        categories TEXT,
        unlock TEXT,
        item_numeric_id INTEGER,
        localization_internal_name TEXT,
        stats TEXT
    );
    """


class RiskOfRain2EquipmentSchema(BaseModel):
    """
    Logical schema for the curated `riskofrain2_equipment` table.
    """

    game_id: str
    equipment_key: str
    equipment_name: Optional[str] = None
    rarity: Optional[str] = None
    quote: Optional[str] = None
    description: Optional[str] = None
    categories: Optional[str] = None
    unlock: Optional[str] = None
    equipment_numeric_id: Optional[int] = None
    localization_internal_name: Optional[str] = None
    stats: Optional[str] = None

    CREATE_TABLE_SQL: ClassVar[str] = """
    CREATE TABLE IF NOT EXISTS riskofrain2_equipment (
        id BIGSERIAL PRIMARY KEY,
        game_id TEXT NOT NULL,
        equipment_key TEXT NOT NULL,
        equipment_name TEXT,
        rarity TEXT,
        quote TEXT,
        description TEXT,
        categories TEXT,
        unlock TEXT,
        equipment_numeric_id INTEGER,
        localization_internal_name TEXT,
        stats TEXT
    );
    """


class GungeonGunExternalSchema(BaseModel):
    """
    Logical schema for the curated `gungeon_guns_external` table.

    This represents an external catalog of Gungeon guns (e.g. scraped
    from community sites), independent of the original game files.
    """

    game_id: str
    game_name: str
    gun_name: str
    flavor_text: Optional[str] = None
    type: Optional[str] = None
    dps: Optional[float] = None
    damage: Optional[float] = None
    fire_rate: Optional[float] = None
    reload_time: Optional[float] = None
    magazine_size: Optional[int] = None
    ammo_capacity: Optional[int] = None
    shot_speed: Optional[float] = None
    range: Optional[float] = None
    force: Optional[float] = None
    spread: Optional[float] = None
    notes: Optional[str] = None
    source: Optional[str] = None

    CREATE_TABLE_SQL: ClassVar[str] = """
    CREATE TABLE IF NOT EXISTS gungeon_guns_external (
        id BIGSERIAL PRIMARY KEY,
        game_id TEXT NOT NULL,
        game_name TEXT NOT NULL,
        gun_name TEXT NOT NULL,
        flavor_text TEXT,
        type TEXT,
        dps DOUBLE PRECISION,
        damage DOUBLE PRECISION,
        fire_rate DOUBLE PRECISION,
        reload_time DOUBLE PRECISION,
        magazine_size INTEGER,
        ammo_capacity INTEGER,
        shot_speed DOUBLE PRECISION,
        range DOUBLE PRECISION,
        force DOUBLE PRECISION,
        spread DOUBLE PRECISION,
        notes TEXT,
        source TEXT
    );
    """


class GungeonItemExternalSchema(BaseModel):
    """
    Logical schema for the curated `gungeon_items_external` table.

    This represents an external catalog of Gungeon items (e.g. scraped
    from community sites), independent of the original game files.
    """

    game_id: str
    game_name: str
    item_name: Optional[str] = None
    pickup: Optional[str] = None
    type: Optional[str] = None
    effect: Optional[str] = None
    source: Optional[str] = None

    CREATE_TABLE_SQL: ClassVar[str] = """
    CREATE TABLE IF NOT EXISTS gungeon_items_external (
        id BIGSERIAL PRIMARY KEY,
        game_id TEXT NOT NULL,
        game_name TEXT NOT NULL,
        item_name TEXT,
        pickup TEXT,
        type TEXT,
        effect TEXT,
        source TEXT
    );
    """

