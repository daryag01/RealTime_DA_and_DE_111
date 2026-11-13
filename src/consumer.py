"""Part B.2-B.5 + Part C - consumer / Python stream processor.

Reads events from the topic as member of consumer group CONSUMER_GROUP, validates the schema
(invalid events go to a dead-letter list instead of crashing the stream), updates running
aggregates and the 5-second event-time tumbling windows, and writes results to output/.

Usage (real Kafka):   python src/consumer.py --backend kafka --idle-timeout 5
"""
import argparse
import json
import logging
import time
from pathlib import Path

from aggregations import JobAggregator
from broker import deserialize, make_consumer
from config import CONSUMER_GROUP, WATERMARK_SECONDS, WINDOW_SECONDS
from schema import validate
from windowing import TumblingWindowAggregator

log = logging.getLogger("consumer")
OUT = Path(__file__).resolve().parent.parent / "output"


def consume(backend="memory", idle_timeout=2.0, out_dir=OUT):
    consumer = make_consumer(backend, CONSUMER_GROUP)
    agg = JobAggregator()
    win = TumblingWindowAggregator(WINDOW_SECONDS, WATERMARK_SECONDS)
    windows, dead_letter, consumed = [], [], 0
    last_msg = time.monotonic()

    while time.monotonic() - last_msg < idle_timeout:
        records = consumer.poll(timeout_ms=200)
        if not records:
            continue
        last_msg = time.monotonic()
        # within a poll, order by event time so cross-partition windows close correctly
        decoded = []
        for part, off, raw in records:
            try:
                decoded.append((part, off, deserialize(raw)))
            except ValueError as exc:
                dead_letter.append({"partition": part, "offset": off, "errors": [f"bad JSON: {exc}"]})
        decoded.sort(key=lambda r: r[2].get("timestamp", ""))
        for part, off, e in decoded:
            consumed += 1
            errs = validate(e)
            if errs:
                dead_letter.append({"event_id": e.get("event_id"), "partition": part,
                                    "offset": off, "errors": errs})
                log.warning("INVALID p%s@%s %s: %s", part, off, e.get("event_id"), "; ".join(errs))
                continue
            log.info("valid   p%s@%s %s %s %-10s %-9s dur=%s cpu=%s", part, off, e["event_id"],
                     e["research_group"], e["job_type"], e["status"], e["duration_sec"], e["cpu_pct"])
            agg.add(e)
            for w in win.add(e):
                windows.append(w)
                log.info("WINDOW CLOSED %s", w)
        consumer.commit()   # at-least-once: commit only after processing the batch

    for w in win.flush():
        windows.append(w)
        log.info("WINDOW CLOSED (end of stream) %s", w)
    consumer.close()

    result = {"consumed": consumed, "valid": consumed - len(dead_letter),
              "dead_letter": dead_letter, "late_events": win.late_events,
              **agg.report(), "windows": windows}
    out_dir.mkdir(exist_ok=True)
    (out_dir / "consumer_results.json").write_text(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", choices=["kafka", "memory"], default="kafka")
    ap.add_argument("--idle-timeout", type=float, default=5.0)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    print(json.dumps(consume(a.backend, a.idle_timeout), indent=2))
