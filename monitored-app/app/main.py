import logging
import time
from contextlib import asynccontextmanager
from time import perf_counter
from uuid import uuid4

from fastapi import Depends, FastAPI, Query, Request
from fastapi.responses import JSONResponse, Response
from fastapi.concurrency import run_in_threadpool
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError, TimeoutError as PoolTimeoutError
from sqlalchemy.orm import Session

from app import database
from app.config import settings
from app.logging_config import configure_logging, request_id_context
from app.metrics import DB_CONNECTION_TIMEOUTS, HTTP_DURATION, HTTP_ERRORS, HTTP_REQUESTS
from app.routes.admin import router as admin_router
from app.routes.orders import router as orders_router
from app.models import ConfigurationChange

logger = logging.getLogger(__name__)


def record_startup_configuration() -> None:
    with database.runtime.session() as db:
        previous = db.scalar(select(ConfigurationChange).where(ConfigurationChange.key == "db_pool_size")
                             .order_by(ConfigurationChange.changed_at.desc(), ConfigurationChange.id.desc()).limit(1))
        if previous is not None and previous.new_value != settings.db_pool_size:
            db.add(ConfigurationChange(key="db_pool_size", previous_value=previous.new_value,
                                       new_value=settings.db_pool_size, actor="application_startup"))
            db.commit()
            logger.info("Startup pool configuration change recorded", extra={
                "event": "startup_configuration_change", "previous_pool_size": previous.new_value,
                "new_pool_size": settings.db_pool_size,
            })


@asynccontextmanager
async def lifespan(_: FastAPI):
    configure_logging(settings.log_path)
    await run_in_threadpool(database.create_tables)
    await run_in_threadpool(record_startup_configuration)
    database.runtime.refresh_pool_metrics()
    logger.info("Monitored API started", extra={"event": "application_started"})
    try:
        yield
    finally:
        logger.info("Monitored API stopping", extra={"event": "application_stopping"})
        await run_in_threadpool(database.runtime.dispose)


app = FastAPI(title="Monitored Orders API", version=settings.app_version, lifespan=lifespan)
app.include_router(orders_router)
app.include_router(admin_router)
@app.get("/metrics", include_in_schema=False)
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.middleware("http")
async def collect_http_metrics(request: Request, call_next):
    request_id = uuid4().hex
    token = request_id_context.set(request_id)
    started = perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        if status_code >= 500:
            HTTP_ERRORS.inc()
        return response
    except Exception:
        HTTP_ERRORS.inc()
        logger.exception("Unhandled request error", extra={
            "event": "unhandled_request_error", "path": request.url.path,
            "method": request.method, "request_id": request_id,
        })
        raise
    finally:
        HTTP_REQUESTS.labels(method=request.method, status_code=str(status_code)).inc()
        HTTP_DURATION.labels(method=request.method).observe(perf_counter() - started)
        request_id_context.reset(token)


@app.exception_handler(PoolTimeoutError)
async def handle_pool_timeout(_: Request, exc: PoolTimeoutError):
    DB_CONNECTION_TIMEOUTS.inc()
    logger.error("Database connection pool checkout timed out", extra={
        "event": "database_connection_timeout", "request_id": request_id_context.get(),
    })
    return JSONResponse(status_code=503, content={
        "detail": "Database connection pool exhausted; retry after the in-flight work completes",
        "error": "database_connection_timeout",
    })


@app.get("/health")
def health():
    return {"status": "ok", "service": settings.monitored_service_id, "version": settings.app_version}


@app.get("/ready")
def readiness(db: Session = Depends(database.get_db)):
    try:
        db.execute(text("SELECT 1"))
    except SQLAlchemyError:
        logger.exception("Readiness database check failed", extra={"event": "readiness_failed"})
        return JSONResponse(status_code=503, content={"status": "not_ready", "dependency": "postgres"})
    database.runtime.refresh_pool_metrics()
    return {"status": "ready", "dependency": "postgres"}


@app.get("/api/test/hold-db-connection")
def hold_db_connection(seconds: int = Query(default=10, ge=1, le=30), db: Session = Depends(database.get_db)):
    if settings.app_environment != "local" or not settings.test_endpoints_enabled:
        return JSONResponse(status_code=404, content={"detail": "Not found"})
    db.execute(text("SELECT 1"))
    logger.warning("Holding a real database connection for local pool testing", extra={
        "event": "database_connection_held", "request_id": request_id_context.get(),
        "held_seconds": seconds,
    })
    try:
        time.sleep(seconds)
    finally:
        db.rollback()
        logger.info("Released held database connection", extra={
            "event": "database_connection_released", "request_id": request_id_context.get(),
        })
    return {"status": "connection_released", "held_seconds": seconds}
