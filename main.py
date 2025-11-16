from __future__ import annotations

from pathlib import Path
import sys


def main() -> None:
    """
    Entry point for the ingestion pipeline.

    Lua-based wiki ingestion is intentionally disabled for now until
    we hook up a more trustworthy source of game data.
    """
    project_root = Path(__file__).resolve().parent
    src_path = project_root / "src"

    if str(src_path) not in sys.path:
        sys.path.append(str(src_path))

    print(
        "Lua wiki ingestion is currently disabled. "
        "Once a new data source is chosen, wire it into this entrypoint."
    )


if __name__ == "__main__":
    main()


