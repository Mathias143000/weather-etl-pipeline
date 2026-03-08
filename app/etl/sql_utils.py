from __future__ import annotations

from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

SUPPORTED_DIALECTS = {"postgresql", "sqlite"}


def get_dialect_name(session: Session) -> str:
    dialect_name = session.get_bind().dialect.name
    if dialect_name not in SUPPORTED_DIALECTS:
        supported = ", ".join(sorted(SUPPORTED_DIALECTS))
        raise RuntimeError(
            f"Unsupported SQL dialect: '{dialect_name}'. "
            f"Supported dialects: {supported}."
        )
    return dialect_name


def insert_for_model(dialect_name: str, model: Any):
    if dialect_name == "postgresql":
        return pg_insert(model)
    if dialect_name == "sqlite":
        return sqlite_insert(model)
    supported = ", ".join(sorted(SUPPORTED_DIALECTS))
    raise RuntimeError(
        f"Unsupported SQL dialect: '{dialect_name}'. "
        f"Supported dialects: {supported}."
    )
