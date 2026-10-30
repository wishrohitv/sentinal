import threading
import time
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.orm import Session, sessionmaker

from app.config import settings
from app.metrics import (
    DB_CONNECTION_TIMEOUTS,
    DB_POOL_CHECKED_IN,
    DB_POOL_CHECKED_OUT,
    DB_POOL_CHECKINS,
    DB_POOL_CHECKOUTS,
    DB_POOL_CONNECTIONS_CREATED,
    DB_POOL_SIZE,
)
from app.models import Base


class DatabaseRuntime:
    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._active_sessions = 0
        self._reconfiguring = False
        if settings.db_pool_size > settings.max_db_pool_size:
            raise ValueError("DB_POOL_SIZE cannot exceed MAX_DB_POOL_SIZE")
        self.engine = self._create_engine(settings.db_pool_size)
        self.session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)
        self._attach_pool_metrics(self.engine)
        self.refresh_pool_metrics()

    def _create_engine(self, pool_size: int) -> Engine:
        return create_engine(
            settings.database_url,
            pool_size=pool_size,
            max_overflow=settings.db_max_overflow,
            pool_timeout=settings.db_pool_timeout,
            pool_pre_ping=True,
        )

    def _attach_pool_metrics(self, engine: Engine) -> None:
        event.listen(engine.pool, "checkout", self._on_checkout)
        event.listen(engine.pool, "checkin", self._on_checkin)
        event.listen(engine.pool, "connect", self._on_connect)

    def _on_checkout(self, dbapi_connection, connection_record, connection_proxy) -> None:
        DB_POOL_CHECKOUTS.inc()
        self.refresh_pool_metrics()

    def _on_checkin(self, dbapi_connection, connection_record) -> None:
        DB_POOL_CHECKINS.inc()
        self.refresh_pool_metrics()

    def _on_connect(self, dbapi_connection, connection_record) -> None:
        DB_POOL_CONNECTIONS_CREATED.inc()
        self.refresh_pool_metrics()

    def refresh_pool_metrics(self) -> None:
        pool = self.engine.pool
        capacity = pool.size() + settings.db_max_overflow
        DB_POOL_SIZE.set(capacity)
        DB_POOL_CHECKED_OUT.set(pool.checkedout())
        DB_POOL_CHECKED_IN.set(pool.checkedin())

    @contextmanager
    def session(self) -> Iterator[Session]:
        with self._condition:
            while self._reconfiguring:
                self._condition.wait()
            self._active_sessions += 1
            factory = self.session_factory
        try:
            with factory() as db:
                yield db
        finally:
            with self._condition:
                self._active_sessions -= 1
                self._condition.notify_all()

    def create_tables(self) -> None:
        Base.metadata.create_all(self.engine)

    def reconfigure_pool(self, new_size: int, drain_timeout: float = 15.0) -> int:
        if new_size <= 0 or new_size > settings.max_db_pool_size:
            raise ValueError(f"Pool size must be between 1 and {settings.max_db_pool_size}")
        with self._condition:
            if self._reconfiguring:
                raise RuntimeError("A pool reconfiguration is already in progress")
            self._reconfiguring = True
            deadline = time.monotonic() + drain_timeout
            while self._active_sessions:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._reconfiguring = False
                    self._condition.notify_all()
                    raise TimeoutError("Timed out draining active database requests")
                self._condition.wait(remaining)

        new_engine: Engine | None = None
        try:
            new_engine = self._create_engine(new_size)
            self._attach_pool_metrics(new_engine)
            with new_engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            with self._condition:
                old_engine = self.engine
                self.engine = new_engine
                self.session_factory = sessionmaker(bind=new_engine, expire_on_commit=False)
            old_engine.dispose()
            self.refresh_pool_metrics()
            return new_size
        except Exception:
            if new_engine is not None:
                new_engine.dispose()
            raise
        finally:
            with self._condition:
                self._reconfiguring = False
                self._condition.notify_all()

    def dispose(self) -> None:
        self.engine.dispose()


runtime = DatabaseRuntime()


def get_db() -> Iterator[Session]:
    with runtime.session() as db:
        yield db


def create_tables() -> None:
    runtime.create_tables()


def record_pool_timeout() -> None:
    DB_CONNECTION_TIMEOUTS.inc()
