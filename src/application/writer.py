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
class GungeonExternalGunIngestionService:
    """
    Application service that uses a generic HTML catalog client to
    retrieve external Enter the Gungeon gun stats as a DataFrame,
    then writes them to Parquet.
    """

    client: HtmlSourceClient

    def fetch_guns_df(self) -> pd.DataFrame:
        items = self.client.fetch_items()
        if not items:
            return pd.DataFrame()

        record_cfg = self._load_record_config()
        records = [self._item_to_record(it, record_cfg) for it in items]
        return pd.DataFrame.from_records(records)

    @staticmethod
    def _load_record_config() -> dict:
        """
        Load the record mapping for external Gungeon guns from YAML.
        """
        config_path = project_root / "config" / "games.yaml"
        with config_path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        games_cfg = raw.get("games") or {}
        gungeon_cfg = games_cfg.get("gungeon") or {}
        external_cfg = gungeon_cfg.get("external_sources") or {}
        tiered_cfg = external_cfg.get("tiereditems") or {}
        guns_cfg = tiered_cfg.get("guns") or {}

        return guns_cfg.get("record") or {}

    @staticmethod
    def _item_to_record(item: HtmlCatalogItem, record_cfg: dict) -> dict:
        """
        Apply the YAML-driven record mapping to a scraped catalog item.
        """
        record: dict = {}
        for out_field, cfg in record_cfg.items():
            cfg = cfg or {}
            if "field" in cfg:
                record[out_field] = getattr(item, cfg["field"], None)
            elif "value" in cfg:
                record[out_field] = cfg["value"]
            else:
                record[out_field] = None
        return record


def main() -> None:
    """
    CLI helper to fetch external Gungeon gun stats (currently from
    tiereditems.com via HTML scraping), write them to a dedicated
    Parquet file, and print a small preview.

        poetry run python src/application/writer.py
    """
    client = HtmlSourceClient(game_id="gungeon", entity="guns", provider="tiereditems")
    service = GungeonExternalGunIngestionService(client=client)
    df = service.fetch_guns_df()
    print(f"Fetched {len(df)} external Gungeon guns.")

    output_dir = Path(__file__).resolve().parents[2] / "data" / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "gungeon_guns_external.parquet"

    if not df.empty:
        df.to_parquet(output_path)
        print(f"Wrote Parquet to {output_path}")
        print(df.head())
    else:
        print("No guns fetched; nothing written.")


if __name__ == "__main__":
    main()


