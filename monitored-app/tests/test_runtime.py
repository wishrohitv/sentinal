from sqlalchemy import text

from app.config import settings
from app.database import DatabaseRuntime


def test_pool_runtime_rebuilds_engine_with_new_validated_size(monkeypatch, tmp_path):
    database_file = tmp_path / "monitored.db"
    monkeypatch.setattr(settings, "database_url", f"sqlite:///{database_file}")
    monkeypatch.setattr(settings, "db_pool_size", 2)
    monkeypatch.setattr(settings, "db_max_overflow", 0)
    monkeypatch.setattr(settings, "max_db_pool_size", 5)

    runtime = DatabaseRuntime()
    try:
        runtime.create_tables()
        assert runtime.engine.pool.size() == 2
        assert runtime.reconfigure_pool(4) == 4
        assert runtime.engine.pool.size() == 4
        with runtime.session() as session:
            assert session.execute(text("SELECT 1")).scalar_one() == 1
    finally:
        runtime.dispose()


def test_pool_runtime_rejects_out_of_range_size(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "database_url", f"sqlite:///{tmp_path / 'monitored.db'}")
    monkeypatch.setattr(settings, "db_pool_size", 2)
    monkeypatch.setattr(settings, "db_max_overflow", 0)
    monkeypatch.setattr(settings, "max_db_pool_size", 5)
    runtime = DatabaseRuntime()
    try:
        try:
            runtime.reconfigure_pool(6)
        except ValueError as exc:
            assert "must be between" in str(exc)
        else:
            raise AssertionError("An out-of-range pool size was accepted")
    finally:
        runtime.dispose()
