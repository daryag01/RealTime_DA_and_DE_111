"""Broker abstraction.

backend="kafka"  -> real Apache Kafka through kafka-python (use with docker-compose.yml)
backend="memory" -> in-process emulation of Kafka semantics (topic, N partitions,
                    key-hash partitioning, consumer group with partition assignment and
                    committed offsets). Used for offline runs and tests when no broker exists.
Both expose the same tiny API so producer/consumer/benchmark code is identical.
"""
import json
import threading
import time
import zlib

from config import BOOTSTRAP_SERVERS, NUM_PARTITIONS, REPLICATION_FACTOR, TOPIC


def serialize(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


def deserialize(raw: bytes) -> dict:
    return json.loads(raw.decode("utf-8"))


# --------------------------------------------------------------------------- in-memory
class _MemoryCluster:
    """Single shared 'cluster': topic -> list of partition logs (append-only lists)."""
    def __init__(self):
        self.topics: dict[str, list[list[bytes]]] = {}
        self.committed: dict[tuple[str, str, int], int] = {}   # (group, topic, partition) -> offset
        self.lock = threading.Lock()

    def create_topic(self, topic, partitions):
        with self.lock:
            self.topics.setdefault(topic, [[] for _ in range(partitions)])


CLUSTER = _MemoryCluster()


class MemoryProducer:
    def __init__(self, topic=TOPIC, partitions=NUM_PARTITIONS):
        CLUSTER.create_topic(topic, partitions)
        self.topic, self.partitions = topic, partitions

    def send(self, key: str, value: bytes) -> tuple[int, int]:
        p = zlib.crc32(key.encode()) % self.partitions      # deterministic key-hash partitioner
        log = CLUSTER.topics[self.topic][p]
        log.append(value)                                    # list.append is atomic under the GIL
        return p, len(log) - 1

    def flush(self):
        pass

    def close(self):
        pass


class MemoryConsumer:
    def __init__(self, group_id, topic=TOPIC, member=0, members=1, max_poll_records=500):
        CLUSTER.create_topic(topic, NUM_PARTITIONS)
        self.group, self.topic = group_id, topic
        n = len(CLUSTER.topics[topic])
        # range-style assignment: member k gets partitions k, k+members, ...
        self.assignment = [p for p in range(n) if p % members == member]
        self.position = {p: CLUSTER.committed.get((group_id, topic, p), 0) for p in self.assignment}
        self.max_poll_records = max_poll_records

    def poll(self, timeout_ms=100):
        """Return list of (partition, offset, value) records."""
        deadline = time.perf_counter() + timeout_ms / 1000
        while True:
            out = []
            for p in self.assignment:
                log = CLUSTER.topics[self.topic][p]
                end = min(len(log), self.position[p] + self.max_poll_records - len(out))
                for off in range(self.position[p], end):
                    out.append((p, off, log[off]))
                self.position[p] = end
                if len(out) >= self.max_poll_records:
                    break
            if out or time.perf_counter() >= deadline:
                return out
            time.sleep(0.0005)

    def commit(self):
        for p, off in self.position.items():
            CLUSTER.committed[(self.group, self.topic, p)] = off

    def close(self):
        self.commit()


# --------------------------------------------------------------------------- real Kafka
def ensure_kafka_topic():
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        admin.create_topics([NewTopic(TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR)])
    except TopicAlreadyExistsError:
        pass
    admin.close()


class KafkaProducerWrapper:
    def __init__(self, **overrides):
        from kafka import KafkaProducer
        ensure_kafka_topic()
        cfg = dict(bootstrap_servers=BOOTSTRAP_SERVERS, acks="all", linger_ms=5,
                   batch_size=32_768, compression_type=None, retries=5)
        cfg.update(overrides)
        self.p = KafkaProducer(**cfg)

    def send(self, key: str, value: bytes):
        md = self.p.send(TOPIC, key=key.encode(), value=value).get(timeout=10) \
            if self._sync else self.p.send(TOPIC, key=key.encode(), value=value)
        return (md.partition, md.offset) if self._sync else (None, None)

    _sync = False

    def flush(self):
        self.p.flush()

    def close(self):
        self.p.close()


class KafkaConsumerWrapper:
    def __init__(self, group_id, max_poll_records=500, **_):
        from kafka import KafkaConsumer
        self.c = KafkaConsumer(TOPIC, bootstrap_servers=BOOTSTRAP_SERVERS, group_id=group_id,
                               auto_offset_reset="earliest", enable_auto_commit=False,
                               max_poll_records=max_poll_records)

    def poll(self, timeout_ms=100):
        out = []
        for tp, recs in self.c.poll(timeout_ms=timeout_ms).items():
            out.extend((tp.partition, r.offset, r.value) for r in recs)
        return out

    def commit(self):
        self.c.commit()

    def close(self):
        self.c.commit()
        self.c.close()


def make_producer(backend: str, sync=False):
    if backend == "kafka":
        p = KafkaProducerWrapper()
        p._sync = sync
        return p
    return MemoryProducer()


def make_consumer(backend: str, group_id: str, **kw):
    return KafkaConsumerWrapper(group_id, **kw) if backend == "kafka" else MemoryConsumer(group_id, **kw)
