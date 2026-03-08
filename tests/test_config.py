import pytest
from pydantic import ValidationError

from app.etl.config import Settings, load_cities_from_yaml


def test_load_cities_from_yaml_success(tmp_path):
    path = tmp_path / "cities.yml"
    path.write_text(
        "cities:\n"
        "  - name: Berlin\n"
        "    latitude: 52.52\n"
        "    longitude: 13.405\n"
        "    timezone: Europe/Berlin\n",
        encoding="utf-8",
    )

    cities = load_cities_from_yaml(path)
    assert len(cities) == 1
    assert cities[0].name == "Berlin"
    assert cities[0].latitude == pytest.approx(52.52)
    assert cities[0].timezone == "Europe/Berlin"


def test_load_cities_from_yaml_missing_file(tmp_path):
    missing = tmp_path / "missing.yml"
    with pytest.raises(FileNotFoundError):
        load_cities_from_yaml(missing)


def test_load_cities_from_yaml_invalid_root(tmp_path):
    path = tmp_path / "cities.yml"
    path.write_text("- name: Berlin\n", encoding="utf-8")

    with pytest.raises(ValueError, match="root must be a mapping"):
        load_cities_from_yaml(path)


def test_load_cities_from_yaml_invalid_cities_field(tmp_path):
    path = tmp_path / "cities.yml"
    path.write_text("cities: wrong_type\n", encoding="utf-8")

    with pytest.raises(ValueError, match="'cities' field must be a list"):
        load_cities_from_yaml(path)


def test_settings_reject_non_positive_values(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite+pysqlite:///:memory:")
    monkeypatch.setenv("TRANSFORM_CLOSED_DAYS_BACK", "0")

    with pytest.raises(ValidationError, match="greater than or equal to 1"):
        Settings()
