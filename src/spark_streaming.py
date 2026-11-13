"""Part D - Spark Structured Streaming equivalent of the windowed transformation.

Output columns: window (struct start/end), job_type, count, avg_duration
Source: Kafka topic (default) or a directory of JSON-lines files (--source files) for runs without Kafka.

Run with Kafka (from the project root, Spark 3.5.x / Scala 2.12):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      src/spark_streaming.py --source kafka
Run without Kafka:
  spark-submit src/spark_streaming.py --source files
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType)

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from config import BOOTSTRAP_SERVERS, TOPIC, WATERMARK_SECONDS, WINDOW_SECONDS  # noqa: E402

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("research_group", StringType(), False),
    StructField("job_type", StringType(), False),
    StructField("duration_sec", IntegerType(), False),
    StructField("status", StringType(), False),
    StructField("cpu_pct", DoubleType(), False),
])


def build(spark, source):
    if source == "kafka":
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
               .option("subscribe", TOPIC)
               .option("startingOffsets", "earliest")
               .option("maxOffsetsPerTrigger", 10_000)
               .load()
               .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
               .select("e.*"))
    else:
        raw = (spark.readStream.schema(EVENT_SCHEMA)
               .option("maxFilesPerTrigger", 1)
               .json(os.path.join(os.path.dirname(__file__), "..", "data", "stream")))

    events = (raw
              .filter(F.col("event_id").isNotNull())                               # drop unparsable
              .filter(F.col("job_type").isin("simulation", "ml", "image", "text"))  # schema rules
              .filter(F.col("status").isin("completed", "failed"))
              .withColumn("event_time", F.to_timestamp("timestamp")))

    return (events
            .withWatermark("event_time", f"{WATERMARK_SECONDS} seconds")
            .groupBy(F.window("event_time", f"{WINDOW_SECONDS} seconds"), F.col("job_type"))
            .agg(F.count("*").alias("count"),
                 F.round(F.avg("duration_sec"), 2).alias("avg_duration"))
            .select(F.date_format("window.start", "HH:mm:ss").alias("window_start"),
                    F.date_format("window.end", "HH:mm:ss").alias("window_end"),
                    "job_type", "count", "avg_duration"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["kafka", "files"], default="kafka")
    ap.add_argument("--mode", choices=["update", "append", "complete"], default="update")
    a = ap.parse_args()

    spark = (SparkSession.builder.appName("aitu-research-windows")
             .config("spark.sql.shuffle.partitions", "3")        # = Kafka partitions, avoid 200 tiny tasks
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    query = (build(spark, a.source).writeStream
             .outputMode(a.mode)
             .format("console")
             .option("truncate", False)
             .option("checkpointLocation", "/tmp/aitu-spark-checkpoint")
             .trigger(processingTime="2 seconds")
             .start())
    query.awaitTermination()
