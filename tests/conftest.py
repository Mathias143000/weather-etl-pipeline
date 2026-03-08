import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db import models  # noqa: F401  (import models so metadata is populated)
from app.db.base import Base


@pytest.fixture()
def engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture()
def session(engine):
    with Session(engine) as db_session:
        db_session.execute(
            models.City.__table__.insert().values(
                id=1,
                name="TestCity",
                latitude=10.0,
                longitude=20.0,
                timezone="UTC",
            )
        )
        db_session.commit()
        yield db_session
