from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import yaml

from domain.config import FetchConfig, GameConfig, SourceConfig
from application.lua_ingestion import LuaIngestionService
from infrastructure.http_client import HttpClient


def load_config(path: str | Path) -> Dict[str, GameConfig]:
    """
    Load the YAML config file and return a mapping of game_id -> GameConfig.

    This proves:
      1) We can load the YAML config.
    """
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    games_section = raw.get("games", {})

    games: Dict[str, GameConfig] = {}
    for game_id, cfg in games_section.items():
        sources_cfg = cfg.get("sources", {})
        sources: Dict[str, SourceConfig] = {}
        for source_name, source in sources_cfg.items():
            fetch_cfg = source.get("fetch", {}) or {}
            sources[source_name] = SourceConfig(
                name=source_name,
                fetch=FetchConfig(
                    type=str(fetch_cfg.get("type", "")),
                    url=fetch_cfg.get("url"),
                ),
                format=str(source.get("format", "")),
            )

        games[game_id] = GameConfig(
            id=str(cfg.get("id", game_id)),
            name=str(cfg.get("name", game_id)),
            sources=sources,
        )

    return games


def run_ingestion_for_game(game_id: str, config_path: str | Path) -> pd.DataFrame:
    """
    Minimal ingestion "pipeline" that:
      1) Loads the YAML config (via `load_config`).
      2) Selects the given game + its sources (simulating injection into the pipeline).
      3) Returns a pandas DataFrame that summarizes the config.

    Later, this function can be extended to actually fetch + parse + load data.
    """
    games = load_config(config_path)
    if game_id not in games:
        raise ValueError(f"Unknown game_id: {game_id!r}. Available: {list(games)}")

    game_cfg = games[game_id]

    # For now, our "ingested data" is just a flattened view of the source configs.
    rows: List[Dict[str, Any]] = []
    for source_name, source_cfg in game_cfg.sources.items():
        rows.append(
            {
                "game_id": game_cfg.id,
                "game_name": game_cfg.name,
                "source_name": source_name,
                "fetch_type": source_cfg.fetch.type,
                "fetch_url": source_cfg.fetch.url,
                "format": source_cfg.format,
            }
        )

    df = pd.DataFrame(rows)
    return df


def demo_gungeon(config_path: str | Path | None = None) -> pd.DataFrame:
    """
    Convenience helper for the notebook:

    In your notebook you can simply do:

        from src.ingestion import demo_gungeon
        df = demo_gungeon()
        df

    This will:
      * Load `config/games.yaml`.
      * Run the minimal ingestion for the 'gungeon' game.
      * Return the pandas DataFrame.
    """
    if config_path is None:
        # Assume we're running from the project root.
        config_path = Path("config") / "games.yaml"

    return run_ingestion_for_game("gungeon", config_path)


def demo_gungeon_data(config_path: str | Path | None = None) -> pd.DataFrame:
    """
    End-to-end demo for the notebook that:
      * Loads the YAML config.
      * Injects it into the Gungeon ingestion application service.
      * Returns a DataFrame of actual items/guns from the wiki Lua tables.
    """
    if config_path is None:
        config_path = Path("config") / "games.yaml"

    games = load_config(config_path)
    game_cfg = games.get("gungeon")
    if game_cfg is None:
        raise ValueError("No 'gungeon' game configuration found.")

    service = LuaIngestionService(http_client=HttpClient())
    return service.ingest(game_cfg)


def demo_riskofrain2_data(config_path: str | Path | None = None) -> pd.DataFrame:
    """
    End-to-end demo for Risk of Rain 2 that:
      * Loads the YAML config.
      * Injects it into the ingestion application service.
      * Returns a DataFrame of items + equipment from the wiki Lua tables.
    """
    if config_path is None:
        config_path = Path("config") / "games.yaml"

    games = load_config(config_path)
    game_cfg = games.get("riskofrain2")
    if game_cfg is None:
        raise ValueError("No 'riskofrain2' game configuration found.")

    service = LuaIngestionService(http_client=HttpClient())
    return service.ingest(game_cfg)


def dump_game_raw_parquet(
    game_id: str,
    config_path: str | Path | None = None,
    output_dir: str | Path = "data/raw",
) -> List[Path]:
    """
    Ingest a game and dump one Parquet file per entity_type as a "raw" snapshot.

    Files are written under `output_dir` as:

        {game_id}__{entity_type}.parquet

    Returns the list of written file paths.
    """
    if config_path is None:
        config_path = Path("config") / "games.yaml"

    games = load_config(config_path)
    game_cfg = games.get(game_id)
    if game_cfg is None:
        raise ValueError(f"No {game_id!r} game configuration found.")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    service = LuaIngestionService(http_client=HttpClient())
    df = service.ingest(game_cfg)

    if df.empty:
        return []

    written_paths: List[Path] = []

    # One file per entity_type (e.g. items, guns, equipment).
    for entity_type, group in df.groupby("entity_type"):
        filename = f"{game_id}__{entity_type}.parquet"
        path = output_dir / filename
        # Let pandas pick the Parquet engine (expects pyarrow/fastparquet to be installed).
        group.to_parquet(path)
        written_paths.append(path)

    return written_paths


def dump_gungeon_raw_parquet(
    config_path: str | Path | None = None,
    output_dir: str | Path = "data/raw",
) -> List[Path]:
    """Convenience wrapper to dump Gungeon raw Parquet snapshots."""
    return dump_game_raw_parquet("gungeon", config_path=config_path, output_dir=output_dir)


def dump_riskofrain2_raw_parquet(
    config_path: str | Path | None = None,
    output_dir: str | Path = "data/raw",
) -> List[Path]:
    """Convenience wrapper to dump Risk of Rain 2 raw Parquet snapshots."""
    return dump_game_raw_parquet("riskofrain2", config_path=config_path, output_dir=output_dir)
