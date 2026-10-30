import hmac
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db, runtime
from app.logging_config import request_id_context
from app.models import ConfigurationChange

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/internal", tags=["local control"])
app_started_at = datetime.now(timezone.utc)


class PoolResizeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    new_size: int = Field(ge=1, le=64)

    @field_validator("new_size")
    @classmethod
    def limit_to_local_max(cls, value: int) -> int:
        if value > settings.max_db_pool_size:
            raise ValueError(f"new_size must not exceed {settings.max_db_pool_size}")
        return value


def require_control_token(authorization: str | None = Header(default=None)) -> None:
    if not settings.control_token:
        raise HTTPException(status_code=503, detail="Local control endpoint is not configured")
    expected = f"Bearer {settings.control_token}"
    if not authorization or not hmac.compare_digest(authorization, expected):
        raise HTTPException(status_code=401, detail="Invalid control credentials")


@router.get("/metadata", dependencies=[Depends(require_control_token)])
def metadata():
    return {
        "service_id": settings.monitored_service_id,
        "service_name": "Monitored Orders API",
        "version": settings.app_version,
        "started_at": app_started_at.isoformat(),
        "database_pool_size": runtime.engine.pool.size(),
        "database_max_overflow": settings.db_max_overflow,
        "database_pool_timeout_seconds": settings.db_pool_timeout,
        "source": "monitored_application_runtime",
    }


@router.get("/configuration-changes", dependencies=[Depends(require_control_token)])
def configuration_changes(db: Session = Depends(get_db)):
    records = list(db.scalars(select(ConfigurationChange).order_by(ConfigurationChange.changed_at.desc()).limit(50)).all())
    return [{
        "id": row.id,
        "key": row.key,
        "previous_value": row.previous_value,
        "new_value": row.new_value,
        "actor": row.actor,
        "changed_at": row.changed_at.isoformat(),
    } for row in records]


@router.post("/remediation/pool-size", dependencies=[Depends(require_control_token)])
def resize_pool(payload: PoolResizeRequest):
    previous_size = runtime.engine.pool.size()
    if payload.new_size <= previous_size:
        raise HTTPException(status_code=422, detail="Pool remediation must increase the current pool size")
    try:
        runtime.reconfigure_pool(payload.new_size)
    except TimeoutError as exc:
        raise HTTPException(status_code=409, detail="Could not drain active database requests safely") from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Pool reconfiguration failed")
        raise HTTPException(status_code=503, detail="Pool reconfiguration failed") from exc

    change = ConfigurationChange(
        key="db_pool_size",
        previous_value=previous_size,
        new_value=payload.new_size,
        actor="sentinel_approved_remediation",
    )
    with runtime.session() as db:
        db.add(change)
        db.commit()
    logger.warning("Database pool configuration updated", extra={
        "event": "database_pool_reconfigured",
        "previous_pool_size": previous_size,
        "new_pool_size": payload.new_size,
        "request_id": request_id_context.get(),
    })
    return {"status": "pool_reconfigured", "previous_size": previous_size, "new_size": payload.new_size}
