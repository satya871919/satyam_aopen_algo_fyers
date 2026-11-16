"""Persistent accounting for Fyers daily API request budgets."""

from sqlalchemy import (
    Column,
    Float,
    Index,
    Integer,
    String,
    delete,
    func,
    insert,
    literal,
    select,
    update,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base

from database.engine_factory import create_db_engine
from utils.broker_backpressure import BrokerBusyError
from utils.logging import get_logger

logger = get_logger(__name__)

engine = create_db_engine()
Base = declarative_base()

RETENTION_SECONDS = 8 * 24 * 60 * 60


class FyersApiRequest(Base):
    """A reserved Fyers data request, retained for rolling-window accounting."""

    __tablename__ = "fyers_api_usage"

    id = Column(Integer, primary_key=True)
    category = Column(String(16), nullable=False)
    reserved_at = Column(Float, nullable=False)

    __table_args__ = (
        Index("ix_fyers_api_usage_category_time", "category", "reserved_at"),
    )


class FyersUsageLock(Base):
    """A row lock that serializes daily reservations across processes."""

    __tablename__ = "fyers_api_usage_lock"

    id = Column(Integer, primary_key=True)


def init_db():
    """Create the persistent Fyers request-accounting table."""
    from database.db_init_helper import init_db_with_logging

    init_db_with_logging(Base, engine, "Fyers API Usage DB", logger)
    lock_exists = (
        select(func.count(FyersUsageLock.id))
        .where(FyersUsageLock.id == 1)
        .scalar_subquery()
    )
    seed_lock = insert(FyersUsageLock).from_select(
        ["id"], select(literal(1)).where(lock_exists == 0)
    )
    with engine.begin() as connection:
        connection.execute(seed_lock)


def reserve_daily_request(category, limit, now, window_seconds):
    """Atomically reserve a request if its category is below its rolling cap."""
    cutoff = now - window_seconds
    try:
        with engine.begin() as connection:
            lock_result = connection.execute(
                update(FyersUsageLock)
                .where(FyersUsageLock.id == 1)
                .values(id=FyersUsageLock.id)
            )
            if lock_result.rowcount != 1:
                raise BrokerBusyError(
                    "OpenAlgo could not verify Fyers' daily request budget, so this "
                    "market-data request was not sent. Restart OpenAlgo and try again."
                )
            connection.execute(
                delete(FyersApiRequest).where(
                    FyersApiRequest.reserved_at < now - RETENTION_SECONDS
                )
            )
            usage_count = connection.scalar(
                select(func.count(FyersApiRequest.id)).where(
                    FyersApiRequest.category == category,
                    FyersApiRequest.reserved_at > cutoff,
                )
            )
            if usage_count >= limit:
                return False
            connection.execute(
                insert(FyersApiRequest).values(category=category, reserved_at=now)
            )
    except SQLAlchemyError as exc:
        logger.exception("Could not reserve a Fyers daily API request")
        raise BrokerBusyError(
            "OpenAlgo could not verify Fyers' daily request budget, so this "
            "market-data request was not sent. Check database availability and try again."
        ) from exc

    return True
