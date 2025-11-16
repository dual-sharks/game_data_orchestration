from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys

import pandas as pd
import yaml

# Ensure project src/ is on sys.path when this module is executed directly.
project_root = Path(__file__).resolve().parents[2]
src_root = project_root / "src"
if str(src_root) not in sys.path:
    sys.path.append(str(src_root))

from infrastructure.html_source_client import HtmlCatalogItem, HtmlSourceClient


@dataclass
class GungeonExternalIngestionService:
    """
    Application service that uses a generic HTML catalog client to
    retrieve external Gungeon data (guns, items, etc.) as a DataFrame,
    then writes them to Parquet using YAML-driven record mappings.
    """

    client: HtmlSourceClient  # game_id/entity/provider select config in YAML

    def fetch_df(self) -> pd.DataFrame:
        items = self.client.fetch_items()
        if not items:
            return pd.DataFrame()

        record_cfg = self._load_record_config()
        records = [self._item_to_record(it, record_cfg) for it in items]
        return pd.DataFrame.from_records(records)

    def _load_record_config(self) -> dict:
        """
        Load the record mapping for this game/entity from YAML.
        """
        config_path = project_root / "config" / "games.yaml"
        with config_path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        games_cfg = raw.get("games") or {}
        game_cfg = games_cfg.get(self.client.game_id) or {}
        external_cfg = game_cfg.get("external_sources") or {}
        provider_cfg = external_cfg.get(self.client.provider) or {}
        entity_cfg = provider_cfg.get(self.client.entity) or {}

        return entity_cfg.get("record") or {}

    @staticmethod
    def _item_to_record(item: HtmlCatalogItem, record_cfg: dict) -> dict:
        """
        Apply the YAML-driven record mapping to a scraped catalog item.
        """
        record: dict = {}

        def parse_numeric(raw: str | None, cast: str | None) -> float | int | None:
            if raw is None:
                return None
            raw = raw.strip()
            if not raw:
                return None
            # Strip some common suffixes
            cleaned = raw.replace("%", "").replace("m", "")
            try:
                if cast == "int":
                    return int(float(cleaned))
                if cast == "float":
                    return float(cleaned)
            except ValueError:
                return None
            return raw

        for out_field, cfg in record_cfg.items():
            cfg = cfg or {}
            if "field" in cfg:
                # Direct attribute on HtmlCatalogItem
                record[out_field] = getattr(item, cfg["field"], None)
            elif "source_field" in cfg:
                # Field from item.fields mapping
                raw_val = item.fields.get(cfg["source_field"])
                cast = cfg.get("cast")
                if cast in ("int", "float"):
                    record[out_field] = parse_numeric(raw_val, cast)
                else:
                    record[out_field] = raw_val
            elif "value" in cfg:
                record[out_field] = cfg["value"]
            else:
                record[out_field] = None

        return record


def main() -> None:
    """
    CLI helper to fetch external Gungeon gun AND item stats (currently from
    tiereditems.com via HTML scraping), write them to dedicated Parquet files,
    and print small previews.

        poetry run python src/application/writer.py
    """
    output_dir = Path(__file__).resolve().parents[2] / "data" / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Guns
    guns_client = HtmlSourceClient(game_id="gungeon", entity="guns", provider="tiereditems")
    guns_service = GungeonExternalIngestionService(client=guns_client)
    df_guns = guns_service.fetch_df()
    print(f"Fetched {len(df_guns)} external Gungeon guns.")
    guns_path = output_dir / "gungeon_guns_external.parquet"
    if not df_guns.empty:
        df_guns.to_parquet(guns_path)
        print(f"Wrote guns Parquet to {guns_path}")
        print(df_guns.head())
    else:
        print("No guns fetched; nothing written for guns.")

    # Items
    items_client = HtmlSourceClient(game_id="gungeon", entity="items", provider="tiereditems")
    items_service = GungeonExternalIngestionService(client=items_client)
    df_items = items_service.fetch_df()
    print(f"Fetched {len(df_items)} external Gungeon items.")
    items_path = output_dir / "gungeon_items_external.parquet"
    if not df_items.empty:
        df_items.to_parquet(items_path)
        print(f"Wrote items Parquet to {items_path}")
        print(df_items.head())
    else:
        print("No items fetched; nothing written for items.")


if __name__ == "__main__":
    main()


