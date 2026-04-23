from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    subprocess.run(
        ["docker", "compose", "exec", "-T", "etl", "python", "-m", "app.etl.run"],
        cwd=ROOT,
        check=True,
    )


if __name__ == "__main__":
    main()
