from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import httpx
from bs4 import BeautifulSoup
import yaml


@dataclass
class HtmlCatalogItem:
    """
    Generic representation of a scraped catalog entry from an HTML page.

    The client is responsible only for extracting text; interpretation
    into domain-specific fields happens in the application layer using
    the `fields` mapping.
    """

    # High-level attributes common to most catalog entries.
    name: str
    flavor_text: str | None
    type: str | None
    notes: str | None

    # Raw key/value fields parsed from "Key :Value" lines, e.g.:
    #   "DPS :16.4", "Damage :6", "Effect :Grants a heart container."
    fields: Dict[str, str]

    # Optional synergies parsed from dedicated sections (e.g. Tiered Items cards).
    # Each synergy dict is expected to contain:
    #   - title: str
    #   - description: str | None
    #   - items: List[str] (names of participating guns/items)
    synergies: List[Dict[str, Any]]


@dataclass
class HtmlSourceConfig:
    """
    Configuration for scraping a specific game/entity from an HTML source,
    loaded from `config/games.yaml`.
    """

    base_url: str
    path: str
    item_selector: str
    image_class: str
    name_class: str
    flavor_class: str
    notes_prefix: str
    exclude_type_contains: List[str]
    include_type_contains: List[str]
    synergy_title_class: str | None = None
    synergy_item_class: str | None = None


@dataclass
class HtmlSourceClient:
    """
    Generic HTML source client that knows how to:

      - Load scraping selectors from `config/games.yaml`
      - Fetch the page for a given game/entity
      - Parse catalog items using those selectors

    Currently used against tiereditems.com for Gungeon guns, but
    configured entirely via YAML.
    """

    timeout: float = 10.0
    game_id: str = "gungeon"
    entity: str = "guns"
    provider: str = "tiereditems"

    def _load_source_config(self) -> HtmlSourceConfig:
        """
        Load the HTML structure and selectors for this game/entity from YAML.
        """
        config_path = Path(__file__).resolve().parents[2] / "config" / "games.yaml"
        with config_path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        games_cfg = raw.get("games") or {}
        game_cfg = games_cfg.get(self.game_id) or {}
        providers_cfg = game_cfg.get("external_sources") or {}
        provider_cfg = providers_cfg.get(self.provider) or {}

        base_url = provider_cfg.get("base_url", "")
        entity_cfg = provider_cfg.get(self.entity) or {}

        return HtmlSourceConfig(
            base_url=str(base_url),
            path=str(entity_cfg.get("path", "")),
            item_selector=str(entity_cfg.get("item_selector", "a")),
            image_class=str(entity_cfg.get("image_class", "")),
            name_class=str(entity_cfg.get("name_class", "")),
            flavor_class=str(entity_cfg.get("flavor_class", "")),
            notes_prefix=str(entity_cfg.get("notes_prefix", "Notes:")),
            exclude_type_contains=list(entity_cfg.get("exclude_type_contains", [])),
            include_type_contains=list(entity_cfg.get("include_type_contains", [])),
            synergy_title_class=str(entity_cfg.get("synergy_title_class", "")) or None,
            synergy_item_class=str(entity_cfg.get("synergy_item_class", "")) or None,
        )

    def fetch_raw_html(self) -> str:
        cfg = self._load_source_config()
        url = cfg.base_url.rstrip("/") + cfg.path
        resp = httpx.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.text

    def fetch_items(self) -> List[HtmlCatalogItem]:
        """
        Fetch and parse all catalog entries from the configured HTML source.
        """
        cfg = self._load_source_config()
        html = self.fetch_raw_html()
        soup = BeautifulSoup(html, "html.parser")

        items: List[HtmlCatalogItem] = []

        # Each item appears as an element matching the configured selector
        # that contains an <img> with the configured image class.
        for a in soup.select(cfg.item_selector):
            img = a.find("img", class_=cfg.image_class)
            if not img:
                continue

            span = a.find("span")
            if not span:
                continue

            item = self._parse_card(span, cfg)
            if item is not None:
                items.append(item)

        return items

    @staticmethod
    def _parse_card(span: Any, cfg: HtmlSourceConfig) -> HtmlCatalogItem | None:
        """
        Parse a <span> block that contains the <p> lines for a catalog card.
        """
        ps = span.find_all("p")
        if not ps:
            return None

        name: str | None = None
        flavor_text: str | None = None
        notes: str | None = None
        stats_raw: Dict[str, str] = {}

        for p in ps:
            text = p.get_text(strip=True)
            if not text:
                continue

            # Name
            if cfg.name_class and cfg.name_class in (p.get("class") or []):
                name = text
                continue

            # Flavor / pickup line
            if cfg.flavor_class and cfg.flavor_class in (p.get("class") or []):
                if text:
                    flavor_text = text
                continue

            # Notes
            if text.startswith(cfg.notes_prefix):
                notes = text[len(cfg.notes_prefix) :].strip()
                continue

            # Generic "Key :Value" lines.
            if ":" in text:
                key, value = text.split(":", 1)
                stats_raw[key.strip()] = value.strip()

        if not name:
            return None

        # Apply optional Type-based filters from config.
        type_val = stats_raw.get("Type")
        if type_val:
            type_lower = type_val.lower()
            if cfg.exclude_type_contains:
                for token in cfg.exclude_type_contains:
                    if token.lower() in type_lower:
                        return None
            if cfg.include_type_contains:
                if not any(token.lower() in type_lower for token in cfg.include_type_contains):
                    return None

        synergies: List[Dict[str, Any]] = []

        # Optionally parse synergies if configured (e.g. Tiered Items cards).
        if cfg.synergy_title_class:
            for title_div in span.find_all("div", class_=cfg.synergy_title_class):
                title = title_div.get_text(strip=True)

                # Description is typically the next <p> sibling after the title div.
                description: str | None = None
                sib = title_div.find_next_sibling()
                while sib is not None and sib.name != "p":
                    sib = sib.find_next_sibling()
                if sib is not None and sib.name == "p":
                    description = sib.get_text(strip=True)

                # Collect synergy participants until the next synergy title or end.
                items: List[str] = []
                current = sib.find_next_sibling() if sib is not None else title_div.find_next_sibling()
                while current is not None:
                    classes = current.get("class") or []
                    if cfg.synergy_title_class in classes:
                        # Start of the next synergy block.
                        break
                    if cfg.synergy_item_class and cfg.synergy_item_class in classes:
                        p_tag = current.find("p")
                        if p_tag is not None:
                            name_text = p_tag.get_text(strip=True)
                            if name_text:
                                items.append(name_text)
                    current = current.find_next_sibling()

                if items or description:
                    synergies.append(
                        {
                            "title": title,
                            "description": description,
                            "items": items,
                        }
                    )

        return HtmlCatalogItem(
            name=name,
            flavor_text=flavor_text,
            type=stats_raw.get("Type"),
            notes=notes,
            fields=stats_raw,
            synergies=synergies,
        )


