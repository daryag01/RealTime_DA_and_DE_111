"""Part B.1 - Kafka producer that publishes the supplied research-job events.

Usage (real Kafka):   python src/producer.py --backend kafka
Events are keyed by research_group so all events of one group land in the same partition
(preserves per-group order) and are spread over the 3 partitions.
"""
import argparse
import json
import logging
from pathlib import Path

from broker import make_producer, serialize
from config import BASE_DATE, PARTITION_KEY, TOPIC, VARIANT

log = logging.getLogger("producer")
DATA = Path(__file__).resolve().parent.parent / "data" / "events.json"


def load_events(path=DATA):
    events = json.loads(Path(path).read_text())
    for e in events:  # accept both 'HH:MM:SS' and full ISO timestamps
        if "T" not in e["timestamp"]:
            e["timestamp"] = f"{BASE_DATE}T{e['timestamp']}"
    return events


def produce(backend="memory", path=DATA, extra_events=()):
    producer = make_producer(backend, sync=True)
    sent = []
    for e in list(load_events(path)) + list(extra_events):
        part, off = producer.send(str(e.get(PARTITION_KEY, "unknown")), serialize(e))
        sent.append((e.get("event_id"), e.get(PARTITION_KEY), part, off))
        log.info("sent %-5s key=%-4s -> %s[p%s] offset=%s", e.get("event_id"), e.get(PARTITION_KEY),
                 TOPIC, part, off)
    producer.flush()
    producer.close()
    return sent


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", choices=["kafka", "memory"], default="kafka")
    ap.add_argument("--data", default=str(DATA))
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    log.info("variant=%s topic=%s", VARIANT, TOPIC)
    produce(a.backend, a.data)
