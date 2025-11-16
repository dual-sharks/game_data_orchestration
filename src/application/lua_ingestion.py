from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd

from domain.config import GameConfig
from infrastructure.http_client import HttpClient
from infrastructure.lua_parser import parse_lua_table


@dataclass
class LuaIngestionService:
    """
    Generic application service for ingesting Lua-based wiki data.

    Responsibilities:
      - Use infrastructure adapters (HTTP + Lua parser) to fetch raw data.
      - Transform raw Lua tables into domain-ish rows.
      - Emit a pandas DataFrame as an application-level result.

    It currently expects sources shaped like:

        items["Some Name"] = { ... }
        equipment["Some Name"] = { ... }
        guns["Some Name"] = { ... }

    and will produce one row per table entry.
    """

    http_client: HttpClient

    def ingest(self, game_cfg: GameConfig) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []

        for source_name, source_cfg in game_cfg.sources.items():
            if source_cfg.fetch.type != "http":
                # For now we only support HTTP sources.
                continue

            if source_cfg.fetch.url is None:
                continue

            if source_cfg.format != "lua_table":
                # Only handle Lua table sources for now.
                continue

            text = self.http_client.get_text(source_cfg.fetch.url)
            table = parse_lua_table(text)

            if not isinstance(table, dict):
                continue

            entity_type = source_name  # e.g. "items", "guns", "equipment"

            for key, value in table.items():
                if not isinstance(value, dict):
                    continue

                # Normalize a small set of common fields; keep the rest as JSON.
                name = value.get("Name") or value.get("name")
                quality = value.get("Quality") or value.get("quality")

                rows.append(
                    {
                        "game_id": game_cfg.id,
                        "game_name": game_cfg.name,
                        "entity_type": entity_type,
                        "external_key": key,
                        "name": name,
                        "quality": quality,
                        "raw": json.dumps(value, ensure_ascii=False),
                    }
                )

        return pd.DataFrame(rows)


