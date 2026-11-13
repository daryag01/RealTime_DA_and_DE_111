"""Schema definition and validation for research-job events."""
from datetime import datetime

SCHEMA = {
    "event_id": str,
    "timestamp": str,
    "research_group": str,
    "job_type": str,
    "duration_sec": (int, float),
    "status": str,
    "cpu_pct": (int, float),
}
JOB_TYPES = {"simulation", "ml", "image", "text"}
STATUSES = {"completed", "failed"}


def validate(event: dict) -> list[str]:
    """Return a list of validation errors (empty list == valid event)."""
    errors = []
    for field, ftype in SCHEMA.items():
        if field not in event:
            errors.append(f"missing field '{field}'")
        elif not isinstance(event[field], ftype) or isinstance(event[field], bool):
            errors.append(f"field '{field}' has wrong type {type(event[field]).__name__}")
    if errors:
        return errors
    extra = set(event) - set(SCHEMA) - {"produced_at_ns"}   # produced_at_ns = benchmark metadata
    if extra:
        errors.append(f"unexpected fields {sorted(extra)}")
    if event["job_type"] not in JOB_TYPES:
        errors.append(f"invalid job_type '{event['job_type']}'")
    if event["status"] not in STATUSES:
        errors.append(f"invalid status '{event['status']}'")
    if event["duration_sec"] < 0:
        errors.append("duration_sec must be >= 0")
    if not 0 <= event["cpu_pct"] <= 100:
        errors.append("cpu_pct must be within 0..100")
    try:
        datetime.fromisoformat(event["timestamp"])
    except ValueError:
        errors.append(f"timestamp '{event['timestamp']}' is not ISO-8601")
    return errors
