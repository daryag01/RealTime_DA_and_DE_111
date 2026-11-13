# Assignment 4 — Distributed Stream Processing (AITU Research Event Analytics)

Variant / task code: **A4-VAR-XX** (set `VARIANT` in `src/config.py` or `AITU_VARIANT` env var)

## Layout
| Path | Purpose |
|---|---|
| `data/events.json` | the 10 supplied events (date pinned to 2026-11-13) |
| `data/stream/events.jsonl` | same events as JSON lines for the Spark file source |
| `src/config.py` | topic `aitu.research.events`, 3 partitions, RF 1, group `research-analytics`, 5 s window, 2 s watermark |
| `src/schema.py` | schema + validation rules |
| `src/broker.py` | Kafka (kafka-python) and in-process Kafka emulation behind one API |
| `src/producer.py` | Part B.1 producer (key = research_group) |
| `src/consumer.py` | Part B.2–B.5 + Part C processor (validation, aggregates, windows, dead-letter) |
| `src/windowing.py` | event-time tumbling windows with watermark |
| `src/spark_streaming.py` | Part D Spark Structured Streaming job |
| `src/spark_reference.py` | Python re-implementation of the Spark query (expected output / cross-check) |
| `src/benchmark.py` | Part E throughput/latency/CPU benchmark |
| `src/make_figures.py` | architecture diagram + charts in `docs/` |
| `tests/test_pipeline.py` | unit tests for Parts B and C |
| `output/` | execution logs and JSON results |

## Run with real Kafka
```bash
pip install -r requirements.txt
docker compose up -d                       # Kafka on localhost:9092, Kafka UI on :8080
python src/consumer.py --backend kafka &   # start the consumer group member
python src/producer.py --backend kafka
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src/spark_streaming.py --source kafka
python src/benchmark.py --backend kafka --rates 10 100 500 --duration 10
```

## Run without Kafka (in-process broker)
```bash
python src/run_pipeline.py                 # producer + consumer, writes output/consumer_results.json
python src/spark_reference.py              # expected Part D table
spark-submit src/spark_streaming.py --source files   # if Spark is installed
python src/benchmark.py --rates 10 100 500 2000 5000 20000 --max-poll 1 --log-each --label baseline
python src/benchmark.py --rates 10 100 500 2000 5000 20000 --label optimized
python src/make_figures.py
python tests/test_pipeline.py
```
