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
class ExternalIngestionService:
    """
    Application service that uses a generic HTML catalog client to
    retrieve external game data (guns, items, etc.) as a DataFrame,
    using YAML-driven record mappings.
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


def get_game_name(game_id: str) -> str:
    """
    Helper to resolve the human-readable game name from config by id.
    """
    config_path = project_root / "config" / "games.yaml"
    with config_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    games_cfg = raw.get("games") or {}
    game_cfg = games_cfg.get(game_id) or {}
    return str(game_cfg.get("name", game_id))


def main() -> None:
    """
    CLI helper to run all configured ingestion jobs:
      - html_entity_to_parquet: scrape cards and write Parquet using record mappings
      - synergies_from_html_cards: derive synergies from cards and write Parquet

        poetry run python src/application/writer.py
    """
    output_root = Path(__file__).resolve().parents[2] / "data" / "raw"
    output_root.mkdir(parents=True, exist_ok=True)

    # Load job definitions from config/pipelines.yaml
    pipelines_path = project_root / "config" / "pipelines.yaml"
    with pipelines_path.open("r", encoding="utf-8") as f:
        pipelines_raw = yaml.safe_load(f) or {}

    jobs = pipelines_raw.get("jobs") or []

    for job in jobs:
        kind = job.get("kind")
        game_id = job.get("game_id")
        provider = job.get("provider")
        entity = job.get("entity")
        parquet_rel = job.get("parquet")

        if not (kind and game_id and provider and entity and parquet_rel):
            print(f"Skipping invalid job definition: {job}")
            continue

        parquet_path = output_root.parent / parquet_rel if not parquet_rel.startswith("data/") else Path(parquet_rel)
        if not parquet_path.is_absolute():
            parquet_path = Path(__file__).resolve().parents[2] / parquet_rel

        print(f"Running job {job.get('id', '')!r}: kind={kind}, game_id={game_id}, entity={entity}")

        client = HtmlSourceClient(game_id=game_id, entity=entity, provider=provider)

        if kind == "html_entity_to_parquet":
            service = ExternalIngestionService(client=client)
            df = service.fetch_df()
            print(f"Fetched {len(df)} records for html_entity_to_parquet.")
            if not df.empty:
                parquet_path.parent.mkdir(parents=True, exist_ok=True)
                df.to_parquet(parquet_path)
                print(f"Wrote entity Parquet to {parquet_path}")
                print(df.head())
            else:
                print("No records fetched; nothing written.")

        elif kind == "synergies_from_html_cards":
            # Derive synergies from the raw HTML cards.
            cards = client.fetch_items()
            game_name = get_game_name(game_id)
            source = provider
            base_column = job.get("base_column", "base_name")

            synergy_records: list[dict] = []
            for card in cards:
                base_name = card.name
                for syn in card.synergies:
                    title = syn.get("title")
                    desc = syn.get("description")
                    for related in syn.get("items", []):
                        synergy_records.append(
                            {
                                "game_id": game_id,
                                "game_name": game_name,
                                base_column: base_name,
                                "synergy_title": title,
                                "synergy_description": desc,
                                "related_item_name": related,
                                "source": source,
                            }
                        )

            df_syn = pd.DataFrame.from_records(synergy_records)
            print(f"Derived {len(df_syn)} synergies for job {job.get('id', '')!r}.")
            if not df_syn.empty:
                parquet_path.parent.mkdir(parents=True, exist_ok=True)
                df_syn.to_parquet(parquet_path)
                print(f"Wrote synergies Parquet to {parquet_path}")
                print(df_syn.head())
            else:
                print("No synergies derived; nothing written.")

        else:
            print(f"Unknown job kind {kind!r}; skipping.")


if __name__ == "__main__":
    main()


