from __future__ import annotations

from pathlib import Path
import sys
import importlib


def main() -> None:
    """
    Simple script version of the current notebook flow:
      - wires up `src/` on sys.path
      - reloads `ingestion`
      - pulls Gungeon + Risk of Rain 2 data
      - writes raw Parquet snapshots for each game
    """
    project_root = Path(__file__).resolve().parent
    src_path = project_root / "src"

    if str(src_path) not in sys.path:
        sys.path.append(str(src_path))

    import ingestion  # type: ignore[import]

    importlib.reload(ingestion)

    from ingestion import (  # type: ignore[import]
        demo_gungeon_data,
        demo_riskofrain2_data,
        dump_gungeon_raw_parquet,
        dump_riskofrain2_raw_parquet,
    )

    df_gun = demo_gungeon_data()
    df_ror2 = demo_riskofrain2_data()

    gun_paths = dump_gungeon_raw_parquet()
    ror2_paths = dump_riskofrain2_raw_parquet()

    print(f"Gungeon rows: {len(df_gun)}")
    print(f"Risk of Rain 2 rows: {len(df_ror2)}")
    print("Gungeon Parquet files:")
    for p in gun_paths:
        print(f"  - {p}")
    print("Risk of Rain 2 Parquet files:")
    for p in ror2_paths:
        print(f"  - {p}")


if __name__ == "__main__":
    main()


