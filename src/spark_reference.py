"""Pure-Python re-implementation of the Spark query in spark_streaming.py.

Used to (1) produce the expected Part D table when Spark is not installed and
(2) cross-check Spark's console output. Same rules: 5 s tumbling window on event time,
group by (window, job_type), count(*) and round(avg(duration_sec), 2).
"""
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from config import WINDOW_SECONDS
from producer import load_events
from schema import validate
from windowing import window_start

OUT = Path(__file__).resolve().parent.parent / "output"

if __name__ == "__main__":
    groups = defaultdict(list)
    for e in load_events():
        if validate(e):
            continue
        ws = window_start(datetime.fromisoformat(e["timestamp"]), WINDOW_SECONDS)
        groups[(ws, e["job_type"])].append(e["duration_sec"])
    rows = []
    for (ws, jt), durs in sorted(groups.items()):
        we = datetime.fromtimestamp(ws.timestamp() + WINDOW_SECONDS)
        rows.append({"window": f"[{ws:%H:%M:%S}, {we:%H:%M:%S})", "job_type": jt,
                     "count": len(durs), "avg_duration": round(sum(durs) / len(durs), 2)})
    print(f"{'window':<22}{'job_type':<12}{'count':>6}{'avg_duration':>14}")
    for r in rows:
        print(f"{r['window']:<22}{r['job_type']:<12}{r['count']:>6}{r['avg_duration']:>14}")
    OUT.mkdir(exist_ok=True)
    (OUT / "spark_reference_output.json").write_text(json.dumps(rows, indent=2))
