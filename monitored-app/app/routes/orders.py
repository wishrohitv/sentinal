import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.database import get_db
from app.logging_config import request_id_context
from app.models import Order

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/orders", tags=["orders"])


class OrderCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    item: str = Field(min_length=1, max_length=120)
    quantity: int = Field(ge=1, le=1000)


class OrderOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    item: str
    quantity: int
    created_at: datetime


@router.get("", response_model=list[OrderOut])
def list_orders(limit: int = Query(default=50, ge=1, le=100), db: Session = Depends(get_db)):
    return list(db.scalars(select(Order).order_by(Order.id.desc()).limit(limit)).all())


@router.post("", response_model=OrderOut, status_code=201)
def create_order(payload: OrderCreate, db: Session = Depends(get_db)):
    order = Order(item=payload.item, quantity=payload.quantity)
    db.add(order)
    db.commit()
    db.refresh(order)
    logger.info("Order created", extra={"event": "order_created", "order_id": order.id,
                                       "request_id": request_id_context.get()})
    return order


@router.get("/{order_id}", response_model=OrderOut)
def get_order(order_id: int, db: Session = Depends(get_db)):
    order = db.get(Order, order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return order

