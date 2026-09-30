from datetime import datetime, timedelta, timezone

SCHEDULE_BUFFER = timedelta(minutes=5)


def as_utc(value: datetime) -> datetime:
    """SQLite returns naive datetimes; stored values are always UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def calculate_next_slot(
    now: datetime,
    last_scheduled: datetime | None,
    interval_hours: int,
    from_time: datetime | None = None,
) -> datetime:
    """Round up to an hour while preserving the buffer and minimum spacing."""
    if interval_hours < 1:
        raise ValueError("Schedule interval must be positive")
    earliest = as_utc(now) + SCHEDULE_BUFFER
    if last_scheduled is not None:
        earliest = max(
            earliest, as_utc(last_scheduled) + timedelta(hours=interval_hours)
        )
    if from_time is not None:
        earliest = max(earliest, as_utc(from_time))
    slot = earliest.replace(minute=0, second=0, microsecond=0)
    return slot if slot == earliest else slot + timedelta(hours=1)
