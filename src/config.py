"""Central configuration for the AITU research-event streaming pipeline.

Change VARIANT to your individual variant / task code before submitting.
"""
import os

VARIANT = os.getenv("AITU_VARIANT", "A4-VAR-XX")          # <-- put your individual task code here

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "aitu.research.events")
NUM_PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "3"))   # one per research group G01..G03
REPLICATION_FACTOR = int(os.getenv("KAFKA_RF", "1"))       # 1 on a single dev broker, 3 in production
CONSUMER_GROUP = os.getenv("KAFKA_GROUP", "research-analytics")
PARTITION_KEY = "research_group"                            # keeps each group's events ordered

WINDOW_SECONDS = 5          # tumbling event-time window size (Part C/D)
WATERMARK_SECONDS = 2       # allowed lateness before a window is finalized

BASE_DATE = "2026-11-13"    # the supplied data only has HH:MM:SS; we pin a date for event time
