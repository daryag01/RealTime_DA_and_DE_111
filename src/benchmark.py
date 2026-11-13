"""Part E - throughput / latency benchmark with synthetic load.

A producer thread generates random valid research-job events at a fixed input rate; a consumer
thread runs the full processor (deserialize -> validate -> aggregate -> 5 s windows).
Latency per event = time processed - time produced (perf_counter_ns stamped by the producer),
so it includes broker queueing, poll interval and processing.

  python src/benchmark.py --backend memory --rates 10 100 500 2000 5000 --duration 10
  python src/benchmark.py --backend kafka  --rates 10 100 500 --duration 10   # against real Kafka
Use --log-each --max-poll 1 to run the unoptimized (per-record, logged) consumer.
"""
import argparse
import json
import logging
import random
import statistics
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

import psutil

import broker
from aggregations import JobAggregator
from broker import deserialize, make_consumer, make_producer, serialize
from config import WATERMARK_SECONDS, WINDOW_SECONDS
from schema import validate
from windowing import TumblingWindowAggregator

OUT = Path(__file__).resolve().parent.parent / "output"
GROUPS = [f"G{i:02d}" for i in range(1, 11)]
TYPES = ["simulation", "ml", "image", "text"]


def synthetic_event(i, t0):
    return {"event_id": f"S{i:07d}",
            "timestamp": (t0 + timedelta(milliseconds=i * 10)).isoformat(timespec="milliseconds"),
            "research_group": random.choice(GROUPS), "job_type": random.choice(TYPES),
            "duration_sec": random.randint(5, 180),
            "status": "failed" if random.random() < 0.15 else "completed",
            "cpu_pct": random.randint(20, 99), "produced_at_ns": time.perf_counter_ns()}


def run_level(backend, rate, duration, max_poll, log_each, run_id):
    broker.CLUSTER = broker._MemoryCluster()        # fresh topic for every level (memory backend)
    group = f"bench-{run_id}-{rate}"
    producer, consumer = make_producer(backend), make_consumer(backend, group, max_poll_records=max_poll)
    agg, win = JobAggregator(), TumblingWindowAggregator(WINDOW_SECONDS, WATERMARK_SECONDS)
    plog = logging.getLogger("bench.event")
    total = int(rate * duration)
    latencies, done = [], threading.Event()
    t0 = datetime(2026, 11, 13, 14, 0, 0)

    def produce():
        start = time.perf_counter()
        for i in range(total):
            target = start + i / rate                  # constant-rate pacing
            delay = target - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
            e = synthetic_event(i, t0)
            producer.send(e["research_group"], serialize(e))
        producer.flush()
        done.set()

    proc = psutil.Process()
    proc.cpu_percent(None)
    cpu_samples = []
    sampler_stop = threading.Event()

    def sample_cpu():
        while not sampler_stop.wait(0.5):
            cpu_samples.append(proc.cpu_percent(None))

    threading.Thread(target=sample_cpu, daemon=True).start()
    pt = threading.Thread(target=produce)
    t_start = time.perf_counter()
    pt.start()
    processed, idle_since = 0, None
    while processed < total:
        recs = consumer.poll(timeout_ms=50)
        if not recs:
            if done.is_set():
                idle_since = idle_since or time.perf_counter()
                if time.perf_counter() - idle_since > 5:
                    break
            continue
        idle_since = None
        for _, _, raw in recs:
            e = deserialize(raw)
            if validate(e):
                continue
            agg.add(e)
            win.add(e)
            if log_each:
                plog.info("event %s %s %s %s", e["event_id"], e["research_group"], e["job_type"], e["status"])
            latencies.append((time.perf_counter_ns() - e["produced_at_ns"]) / 1e6)
            processed += 1
        consumer.commit()
    elapsed = time.perf_counter() - t_start
    pt.join()
    sampler_stop.set()
    consumer.close()
    producer.close()
    lat = sorted(latencies)
    return {"input_rate": rate, "events": processed, "elapsed_s": round(elapsed, 2),
            "throughput_eps": round(processed / elapsed, 1),
            "avg_latency_ms": round(statistics.mean(lat), 2),
            "p95_latency_ms": round(lat[int(0.95 * (len(lat) - 1))], 2),
            "max_latency_ms": round(lat[-1], 2),
            "cpu_pct": round(statistics.mean(cpu_samples), 1) if cpu_samples else None}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", choices=["kafka", "memory"], default="memory")
    ap.add_argument("--rates", type=int, nargs="+", default=[10, 100, 500])
    ap.add_argument("--duration", type=float, default=10)
    ap.add_argument("--max-poll", type=int, default=500)
    ap.add_argument("--log-each", action="store_true")
    ap.add_argument("--label", default="optimized")
    a = ap.parse_args()
    random.seed(42)
    OUT.mkdir(exist_ok=True)
    if a.log_each:      # per-event logging to a file, flushed per record (typical naive consumer)
        h = logging.FileHandler(OUT / f"bench_events_{a.label}.log", mode="w")
        logging.getLogger("bench.event").addHandler(h)
        logging.getLogger("bench.event").setLevel(logging.INFO)
    rows = []
    print(f"{'rate':>6} {'events':>7} {'thrpt/s':>9} {'avg ms':>8} {'p95 ms':>8} {'max ms':>8} {'cpu%':>6}")
    for r in a.rates:
        res = run_level(a.backend, r, a.duration, a.max_poll, a.log_each, a.label)
        rows.append(res)
        print(f"{res['input_rate']:>6} {res['events']:>7} {res['throughput_eps']:>9} "
              f"{res['avg_latency_ms']:>8} {res['p95_latency_ms']:>8} {res['max_latency_ms']:>8} "
              f"{res['cpu_pct']:>6}", flush=True)
    meta = {"label": a.label, "backend": a.backend, "max_poll": a.max_poll, "log_each": a.log_each,
            "duration_s": a.duration, "cpu_count": psutil.cpu_count(), "run_at": datetime.now().isoformat()}
    (OUT / f"benchmark_{a.label}.json").write_text(json.dumps({"meta": meta, "results": rows}, indent=2))
