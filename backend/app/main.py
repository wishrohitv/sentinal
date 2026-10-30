import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from app.api.router import router
from app.database import engine, Base, SessionLocal
from app import models  # noqa: F401 - registers all ORM metadata
from app.simulator.state import initialize_simulator
from app.config import settings
from app.models import Service
from app.detection.detector import detect_incident
from app.telemetry.base import TelemetryError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
active_investigation_tasks: set[asyncio.Task] = set()

async def initialize_real_service_record() -> None:
    async with SessionLocal() as session:
        service = await session.get(Service, settings.monitored_service_id)
        if service is None:
            session.add(Service(id=settings.monitored_service_id, name="Monitored Orders API", status="unknown"))
            await session.commit()


async def poll_real_telemetry() -> None:
    while True:
        try:
            async with SessionLocal() as session:
                incident = await detect_incident(session)
                if incident and incident.status == "active":
                    incident.status = "investigating"
                    await session.commit()
                    from app.api.router import run_investigation, selected_provider
                    task = asyncio.create_task(run_investigation(incident.id, selected_provider))
                    active_investigation_tasks.add(task)
                    task.add_done_callback(active_investigation_tasks.discard)
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.getLogger(__name__).exception("Real telemetry detection poll failed")
        await asyncio.sleep(settings.sentinel_detection_interval_seconds)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    if settings.telemetry_provider == "prometheus_loki":
        await initialize_real_service_record()
        detector_task = asyncio.create_task(poll_real_telemetry(), name="sentinel-real-telemetry-detector")
    else:
        async with SessionLocal() as session:
            await initialize_simulator(session)
        detector_task = None
    yield
    if detector_task:
        detector_task.cancel()
        try:
            await detector_task
        except asyncio.CancelledError:
            pass
    for task in active_investigation_tasks:
        task.cancel()
    if active_investigation_tasks:
        await asyncio.gather(*active_investigation_tasks, return_exceptions=True)
    await engine.dispose()

app = FastAPI(title="Sentinel API", version="0.1.0", lifespan=lifespan)
origins = [origin.strip() for origin in settings.cors_origins.split(",") if origin.strip()]
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])
app.include_router(router, prefix="/api")

@app.exception_handler(TelemetryError)
async def telemetry_unavailable(_: Request, exc: TelemetryError):
    return JSONResponse(status_code=503, content={"detail": str(exc), "telemetry_available": False})

@app.get("/")
async def root():
    return {"name": "Sentinel", "docs": "/docs", "health": "/api/health"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000)
