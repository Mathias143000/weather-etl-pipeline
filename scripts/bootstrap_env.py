from __future__ import annotations

import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / ".env"
ENV_EXAMPLE = ROOT / ".env.example"


def main() -> None:
    if ENV_FILE.exists():
        print(".env already exists")
        return

    shutil.copyfile(ENV_EXAMPLE, ENV_FILE)
    print("Created .env from .env.example")


if __name__ == "__main__":
    main()
