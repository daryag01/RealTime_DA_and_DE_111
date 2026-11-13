"""Running (stateful) aggregations for Part B."""
from collections import defaultdict


class JobAggregator:
    def __init__(self):
        self.status = defaultdict(int)
        self.by_type = defaultdict(lambda: {"completed": 0, "failed": 0, "dur_sum": 0.0, "n": 0})
        self.by_group = defaultdict(lambda: {"cpu_sum": 0.0, "n": 0})

    def add(self, e: dict):
        self.status[e["status"]] += 1
        t = self.by_type[e["job_type"]]
        t[e["status"]] += 1
        t["dur_sum"] += e["duration_sec"]
        t["n"] += 1
        g = self.by_group[e["research_group"]]
        g["cpu_sum"] += e["cpu_pct"]
        g["n"] += 1

    def report(self) -> dict:
        order = ["ml", "image", "text", "simulation"]
        return {
            "status_counts": {"completed": self.status["completed"], "failed": self.status["failed"]},
            "by_job_type": [
                {"job_type": jt, "completed": v["completed"], "failed": v["failed"],
                 "avg_duration": round(v["dur_sum"] / v["n"], 2)}
                for jt in order + sorted(set(self.by_type) - set(order))
                if (v := self.by_type.get(jt))
            ],
            "avg_cpu_by_group": [
                {"research_group": g, "events": v["n"], "avg_cpu": round(v["cpu_sum"] / v["n"], 2)}
                for g, v in sorted(self.by_group.items())
            ],
        }
