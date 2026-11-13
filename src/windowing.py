"""Event-time tumbling windows with a watermark (Part C).

Window assignment: start = floor(event_time / W) * W, end = start + W, interval [start, end).
So an event at 14:00:05 belongs to [14:00:05, 14:00:10), not to the first window.
A window is emitted (finalized) once watermark = max_event_time - lateness >= window end.
Events older than the watermark for an already-closed window are counted as 'late' and dropped,
which is the same semantics Spark uses with withWatermark().
"""
from collections import defaultdict
from datetime import datetime, timedelta


def window_start(ts: datetime, size: int) -> datetime:
    epoch = int(ts.timestamp())
    return datetime.fromtimestamp(epoch - epoch % size)


class TumblingWindowAggregator:
    def __init__(self, size_sec=5, lateness_sec=2):
        self.size = size_sec
        self.lateness = timedelta(seconds=lateness_sec)
        self.open = defaultdict(lambda: {"events": [], "failed": 0, "cpu_sum": 0.0})
        self.max_event_time = None
        self.closed_until = None      # end of last emitted window
        self.late_events = []

    def add(self, event: dict) -> list[dict]:
        ts = datetime.fromisoformat(event["timestamp"])
        start = window_start(ts, self.size)
        if self.closed_until and start < self.closed_until:
            self.late_events.append(event["event_id"])
            return []
        w = self.open[start]
        w["events"].append(event["event_id"])
        w["failed"] += event["status"] == "failed"
        w["cpu_sum"] += event["cpu_pct"]
        self.max_event_time = max(self.max_event_time or ts, ts)
        return self._emit(self.max_event_time - self.lateness)

    def _emit(self, watermark) -> list[dict]:
        ready = sorted(s for s in self.open if s + timedelta(seconds=self.size) <= watermark)
        return [self._finalize(s) for s in ready]

    def flush(self) -> list[dict]:
        """End of stream: close every open window."""
        return [self._finalize(s) for s in sorted(self.open)]

    def _finalize(self, start) -> dict:
        w = self.open.pop(start)
        end = start + timedelta(seconds=self.size)
        self.closed_until = max(self.closed_until or end, end)
        n = len(w["events"])
        return {
            "window": f"{start:%H:%M:%S}-{end:%H:%M:%S}",
            "events": ",".join(w["events"]),
            "count": n,
            "failure_rate_pct": round(100 * w["failed"] / n, 2),
            "avg_cpu": round(w["cpu_sum"] / n, 2),
        }
