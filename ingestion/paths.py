"""
Where every Delta table lives, in one place.

Everything hangs off LAKEHOUSE_ROOT so moving the lakehouse (a different
disk, an object-store bucket in Phase 7) is one environment variable, not a
search through every script. Layers are folders: reference/, bronze/,
silver/, gold/ -- the medallion layout is visible in the file system.

The default is <repo>/data/lakehouse, worked out from this file's location
rather than hard-coded, so the same code finds the tables inside the
container (/home/jovyan/work/data/lakehouse) and in a local .venv on the
host (~/.../market-lakehouse/data/lakehouse) with no configuration.
"""

import os
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent

LAKEHOUSE_ROOT = os.getenv("LAKEHOUSE_ROOT", str(_REPO_ROOT / "data" / "lakehouse"))

SECURITY_MASTER = os.getenv("SECURITY_MASTER_PATH", f"{LAKEHOUSE_ROOT}/reference/security_master")

BRONZE_BARS = {
    "alpaca": f"{LAKEHOUSE_ROOT}/bronze/alpaca_bars",
    "yahoo": f"{LAKEHOUSE_ROOT}/bronze/yahoo_bars",
}
