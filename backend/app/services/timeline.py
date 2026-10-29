from typing import Any
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import IncidentEvent

async def add_event(session: AsyncSession, incident_id: str, event_type: str, payload: dict[str, Any]) -> IncidentEvent:
    event = IncidentEvent(incident_id=incident_id, event_type=event_type, payload=payload)
    session.add(event)
    await session.flush()
    return event

async def get_events(session: AsyncSession, incident_id: str, after_id: int = 0) -> list[IncidentEvent]:
    result = await session.scalars(select(IncidentEvent).where(IncidentEvent.incident_id == incident_id,
        IncidentEvent.id > after_id).order_by(IncidentEvent.id))
    return list(result.all())
