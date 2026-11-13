"""Unit tests: python -m pytest tests  (or python tests/test_pipeline.py)"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from aggregations import JobAggregator
from producer import load_events
from schema import validate
from windowing import TumblingWindowAggregator

EV = load_events()


def test_all_supplied_events_valid():
    assert all(validate(e) == [] for e in EV)


def test_invalid_events_rejected():
    bad = dict(EV[0], job_type="quantum", cpu_pct=140)
    errs = validate(bad)
    assert any("job_type" in e for e in errs) and any("cpu_pct" in e for e in errs)
    assert validate({k: v for k, v in EV[0].items() if k != "status"}) == ["missing field 'status'"]


def test_part_b_aggregates():
    a = JobAggregator()
    for e in EV:
        a.add(e)
    r = a.report()
    assert r["status_counts"] == {"completed": 8, "failed": 2}
    t = {x["job_type"]: x for x in r["by_job_type"]}
    assert (t["ml"]["completed"], t["ml"]["failed"], t["ml"]["avg_duration"]) == (2, 1, 52.33)
    assert t["simulation"]["avg_duration"] == 107.5
    g = {x["research_group"]: x["avg_cpu"] for x in r["avg_cpu_by_group"]}
    assert g == {"G01": 68.5, "G02": 62.0, "G03": 86.33}


def test_part_c_windows_boundary_and_values():
    w = TumblingWindowAggregator(5, 2)
    out = []
    for e in EV:
        out += w.add(e)
    out += w.flush()
    assert [(x["window"], x["count"], x["failure_rate_pct"], x["avg_cpu"]) for x in out] == [
        ("14:00:00-14:00:05", 4, 25.0, 66.25),
        ("14:00:05-14:00:10", 5, 20.0, 75.2),
        ("14:00:10-14:00:15", 1, 0.0, 78.0)]


def test_late_event_dropped_after_watermark():
    w = TumblingWindowAggregator(5, 2)
    for e in EV:
        w.add(e)
    w.add(dict(EV[0], event_id="LATE", timestamp="2026-11-13T14:00:03"))
    assert w.late_events == ["LATE"]


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn(); print("PASS", name)
