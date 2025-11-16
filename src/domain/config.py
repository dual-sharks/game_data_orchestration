from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass
class FetchConfig:
    type: str
    url: str | None = None


@dataclass
class SourceConfig:
    name: str
    fetch: FetchConfig
    format: str


@dataclass
class GameConfig:
    id: str
    name: str
    sources: Dict[str, SourceConfig]


