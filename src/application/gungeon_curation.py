from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Optional

import pandas as pd


_NUM_RE = re.compile(r"[-+]?\d*\.?\d+")


def _parse_numeric_maybe(value: Optional[str]) -> Optional[float]:
    """
    Best-effort parser for numeric-ish strings like:
      "1", "+1", "4.5%", "+2.25%", "3m"
    Returns a float or None if it can't confidently parse.
    """
    if not isinstance(value, str):
        return None
    match = _NUM_RE.search(value)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


@dataclass
class GungeonCurationService:
    """
    Application-level curation logic for Gungeon entities.

    Takes the generic raw entities DataFrame (from Parquet or raw table)
    and produces curated, game-specific views.
    """

    def curate_guns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Build a curated Gungeon guns table from the generic raw entities DataFrame.
        """
        mask = (df["game_id"] == "gungeon") & (df["entity_type"] == "guns")
        subset = df.loc[mask].copy()

        if subset.empty:
            return pd.DataFrame()

        def _extract(row: pd.Series) -> pd.Series:
            payload = json.loads(row["raw"])

            rarity = payload.get("Rarity")
            quote = payload.get("Quote")
            desc = payload.get("Desc")
            unlock = payload.get("Unlock")
            corrupt = payload.get("Corrupt")
            internal_name = payload.get("LocalizationInternalName")
            gun_id = payload.get("ID")

            # Category is typically a dict like {"1": "Utility"}.
            category = payload.get("Category")
            if isinstance(category, dict):
                # Stable order by key.
                categories = ", ".join(
                    v for _, v in sorted(category.items(), key=lambda kv: kv[0])
                )
            else:
                categories = None

            stats = payload.get("Stats")
            stats_json = json.dumps(stats, ensure_ascii=False) if stats is not None else None

            # external_key in the raw table is the wiki key/name.
            return pd.Series(
                {
                    "game_id": row["game_id"],
                    "gun_key": row["external_key"],
                    "gun_name": row.get("name"),
                    "rarity": rarity,
                    "quote": quote,
                    "description": desc,
                    "categories": categories,
                    "unlock": unlock,
                    "corrupt": corrupt,
                    "gun_numeric_id": gun_id,
                    "localization_internal_name": internal_name,
                    "stats": stats_json,
                }
            )

        curated = subset.apply(_extract, axis=1)

        # Ensure numeric ID is actually numeric so the DB INTEGER column matches.
        if "gun_numeric_id" in curated.columns:
            curated["gun_numeric_id"] = (
                pd.to_numeric(curated["gun_numeric_id"], errors="coerce").astype("Int64")
            )

        return curated

    def curate_gun_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Build a 1:N stats table for Gungeon guns.
        """
        guns = self.curate_guns(df)
        if guns.empty:
            return pd.DataFrame()

        records = []

        for _, gun_row in guns.iterrows():
            stats_json = gun_row.get("stats")
            if stats_json is None:
                continue

            try:
                stats_payload = json.loads(stats_json)
            except (TypeError, json.JSONDecodeError):
                continue

            if not isinstance(stats_payload, dict):
                continue

            for idx_key, stat_dict in stats_payload.items():
                if not isinstance(stat_dict, dict):
                    continue

                stat_name = stat_dict.get("Stat")
                value_raw = stat_dict.get("Value")
                stack_mode = stat_dict.get("Stack")
                add_raw = stat_dict.get("Add")

                try:
                    idx = int(idx_key)
                except (TypeError, ValueError):
                    idx = None

                records.append(
                    {
                        "game_id": gun_row["game_id"],
                        "gun_key": gun_row["gun_key"],
                        "gun_name": gun_row.get("gun_name"),
                        "idx": idx,
                        "stat_name": stat_name,
                        "value_raw": value_raw,
                        "value_numeric": _parse_numeric_maybe(value_raw),
                        "stack_mode": stack_mode,
                        "add_raw": add_raw,
                        "add_numeric": _parse_numeric_maybe(add_raw),
                    }
                )

        if not records:
            return pd.DataFrame()

        stats_df = pd.DataFrame.from_records(records)

        # Normalize dtypes.
        if "idx" in stats_df.columns:
            stats_df["idx"] = (
                pd.to_numeric(stats_df["idx"], errors="coerce").astype("Int64")
            )

        if "value_numeric" in stats_df.columns:
            stats_df["value_numeric"] = pd.to_numeric(
                stats_df["value_numeric"], errors="coerce"
            )

        if "add_numeric" in stats_df.columns:
            stats_df["add_numeric"] = pd.to_numeric(
                stats_df["add_numeric"], errors="coerce"
            )

        return stats_df


