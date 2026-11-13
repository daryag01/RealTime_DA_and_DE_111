"""Run producer + consumer end to end in one process.

With --backend memory this works without any Kafka installation (the in-process broker is shared).
Two deliberately malformed events are appended to demonstrate schema validation / dead-lettering;
pass --no-bad-events to send only the 10 supplied events.
"""
import argparse
import json
import logging

from consumer import consume
from producer import produce

BAD_EVENTS = [
    {"event_id": "X001", "timestamp": "2026-11-13T14:00:11", "research_group": "G02",
     "job_type": "quantum", "duration_sec": 30, "status": "completed", "cpu_pct": 50},
    {"event_id": "X002", "timestamp": "2026-11-13T14:00:12", "research_group": "G01",
     "job_type": "ml", "duration_sec": "fast", "status": "running", "cpu_pct": 140},
]

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", choices=["kafka", "memory"], default="memory")
    ap.add_argument("--no-bad-events", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-8s %(message)s")
    produce(a.backend, extra_events=[] if a.no_bad_events else BAD_EVENTS)
    res = consume(a.backend, idle_timeout=1.0)
    print(json.dumps({k: res[k] for k in ("consumed", "valid", "status_counts", "by_job_type",
                                          "avg_cpu_by_group", "windows", "dead_letter")}, indent=2))
