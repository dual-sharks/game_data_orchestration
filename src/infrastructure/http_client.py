from __future__ import annotations

from dataclasses import dataclass

import httpx


@dataclass
class HttpClient:
    """
    Very small HTTP client abstraction.

    In a richer DDD setup this would be an infrastructure adapter
    behind a port interface defined in the domain or application layer.
    """

    timeout: float = 10.0

    def get_text(self, url: str) -> str:
        resp = httpx.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.text


