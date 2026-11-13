const fs = require("fs");
const path = require("path");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell, ImageRun, HeadingLevel,
  AlignmentType, WidthType, ShadingType, BorderStyle, LevelFormat, PageBreak, Footer, PageNumber,
} = require("docx");

const ROOT = path.join(__dirname, "..");
const J = (f) => JSON.parse(fs.readFileSync(path.join(ROOT, "output", f)));
const res = J("consumer_results.json");
const spark = J("spark_reference_output.json");
const base = J("benchmark_baseline.json").results;
const opt = J("benchmark_optimized.json").results;
const ceil = J("benchmark_optimized_ceiling.json").results;

const FONT = "Arial";
const W = 9638; // A4 content width with 2 cm margins (DXA)
const HEAD_FILL = "D9E6F5";
const border = { style: BorderStyle.SINGLE, size: 4, color: "A6A6A6" };
const borders = { top: border, bottom: border, left: border, right: border };

const run = (t, o = {}) => new TextRun({ text: String(t), font: FONT, size: o.size || 20, bold: o.bold, italics: o.italics, color: o.color });
function para(content, o = {}) {
  const runs = Array.isArray(content) ? content.map((c) => (typeof c === "string" ? run(c, o) : c)) : [run(content, o)];
  return new Paragraph({ children: runs, spacing: { after: o.after ?? 100, before: o.before ?? 0 }, alignment: o.align, keepNext: o.keepNext });
}
// rich: "**bold** text"
function rich(text, o = {}) {
  const parts = text.split(/(\*\*[^*]+\*\*|`[^`]+`)/).filter(Boolean).map((p) =>
    p.startsWith("**") ? run(p.slice(2, -2), { ...o, bold: true })
      : p.startsWith("`") ? new TextRun({ text: p.slice(1, -1), font: "Consolas", size: (o.size || 20) - 1, color: "1C5CAB" })
      : run(p, o));
  return new Paragraph({ children: parts, spacing: { after: o.after ?? 100 } });
}
const bullet = (t) => new Paragraph({ numbering: { reference: "bullets", level: 0 }, spacing: { after: 60 },
  children: rich(t).options?.children || [] });
function bulletRich(t) {
  const p = rich(t);
  return new Paragraph({ numbering: { reference: "bullets", level: 0 }, spacing: { after: 60 }, children: p.root.filter((c) => c instanceof TextRun) });
}
const h1 = (t) => new Paragraph({ heading: HeadingLevel.HEADING_1, children: [run(t, { size: 30, bold: true })] });
const h2 = (t) => new Paragraph({ heading: HeadingLevel.HEADING_2, children: [run(t, { size: 24, bold: true })] });

function cell(content, width, o = {}) {
  const paras = (Array.isArray(content) ? content : [content]).map((c) =>
    c instanceof Paragraph ? c : (o.rich ? rich(String(c), { size: 18, after: 40 }) :
      new Paragraph({ alignment: o.align, spacing: { after: 20 }, children: [run(c, { size: 18, bold: o.bold })] })));
  return new TableCell({ children: paras, width: { size: width, type: WidthType.DXA }, borders,
    shading: o.fill ? { fill: o.fill, type: ShadingType.CLEAR, color: "auto" } : undefined,
    margins: { top: 60, bottom: 60, left: 100, right: 100 } });
}
function table(headers, rows, widths, o = {}) {
  const total = widths.reduce((a, b) => a + b, 0);
  return new Table({
    width: { size: total, type: WidthType.DXA }, columnWidths: widths,
    rows: [
      ...(headers ? [new TableRow({ tableHeader: true, children: headers.map((h, i) => cell(h, widths[i], { bold: true, fill: HEAD_FILL })) })] : []),
      ...rows.map((r) => new TableRow({ children: r.map((c, i) => cell(c, widths[i], { rich: o.rich, fill: o.firstColFill && i === 0 ? "F0EFEC" : undefined, bold: o.firstColFill && i === 0, align: o.alignRight && i > 0 ? AlignmentType.RIGHT : undefined })) })),
    ],
  });
}
const gap = () => new Paragraph({ children: [], spacing: { after: 120 } });
function img(file, wIn) {
  const buf = fs.readFileSync(path.join(ROOT, "docs", file));
  const pw = buf.readUInt32BE(16), ph = buf.readUInt32BE(20);
  const w = wIn * 96, h = Math.round((w * ph) / pw);
  return new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 60 },
    children: [new ImageRun({ type: "png", data: buf, transformation: { width: w, height: h },
      altText: { title: file, description: file, name: file } })] });
}
const caption = (t) => para(t, { italics: true, size: 18, color: "52514E", align: AlignmentType.CENTER, after: 200 });
const evidence = (rows) => table(["Required evidence", "Student response / reference"],
  rows, [2600, W - 2600], { rich: true, firstColFill: true });
const code = (lines) => lines.map((l) => new Paragraph({ spacing: { after: 0 }, shading: { fill: "F4F4F2", type: ShadingType.CLEAR, color: "auto" },
  children: [new TextRun({ text: l || " ", font: "Consolas", size: 17 })] }));

// ---------------------------------------------------------------- data tables
const jt = res.by_job_type;
const partB = jt.map((t) => [t.job_type, t.completed, t.failed, t.avg_duration.toFixed(2) + " s"]);
const cpuG = res.avg_cpu_by_group.map((g) => [g.research_group, g.events, g.avg_cpu.toFixed(2) + " %"]);
const win = res.windows.map((w) => [w.window.replace("-", "–"), w.events, w.count, w.failure_rate_pct.toFixed(1) + " %", w.avg_cpu.toFixed(2) + " %"]);
const sparkRows = spark.map((r) => [r.window, r.job_type, r.count, r.avg_duration.toFixed(2)]);
const bott = {
  10: "None — idle; latency set by poll interval",
  100: "None — consumer idle most of the time",
  500: "None — ~10 % of one core used",
};
const perfRows = opt.filter((r) => [10, 100, 500].includes(r.input_rate)).map((r) =>
  [r.input_rate, `${r.throughput_eps} ev/s`, `${r.avg_latency_ms} (p95 ${r.p95_latency_ms})`, `${r.cpu_pct} %`, bott[r.input_rate]]);
const both = base.map((b, i) => [b.input_rate, b.throughput_eps, b.avg_latency_ms, b.p95_latency_ms, b.cpu_pct, opt[i].throughput_eps, opt[i].avg_latency_ms, opt[i].p95_latency_ms, opt[i].cpu_pct]);
const b20 = base.find((r) => r.input_rate === 20000), o20 = opt.find((r) => r.input_rate === 20000);
const o5k = opt.find((r) => r.input_rate === 5000), b5k = base.find((r) => r.input_rate === 5000);

// ---------------------------------------------------------------- document
const children = [
  para("Assignment 4 — Distributed Stream Processing", { size: 36, bold: true, align: AlignmentType.CENTER, after: 60 }),
  para("AITU Research Event Analytics with Kafka and Spark", { size: 26, align: AlignmentType.CENTER, after: 60 }),
  para("Astana IT University | Real-time Data Analysis | 2026–2027", { size: 20, color: "52514E", align: AlignmentType.CENTER, after: 120 }),
  para("All data in this report is the synthetic educational dataset supplied with the assignment plus synthetic benchmark load generated by src/benchmark.py.", { italics: true, size: 18, color: "52514E", align: AlignmentType.CENTER, after: 240 }),

  h1("1. Student and Submission Information"),
  table(["Required field", "Student entry"], [
    ["Student full name", ""], ["Student ID", ""], ["Group / cohort", ""], ["Instructor", ""],
    ["Submission date", "13 / 11 / 2026"],
    ["Individual variant / task code", "A4-VAR-XX (set in src/config.py → VARIANT)"],
    ["Git repository / project link", ""],
    ["Python / tool versions", "Python 3.13.18, OpenJDK 21.0.12, psutil 7.2.2, matplotlib 3.11.2; target runtime: kafka-python-ng 2.2.3, Apache Kafka 3.7 (KRaft, Docker), PySpark 3.5.1"],
  ], [3400, W - 3400], { firstColFill: true }),
  gap(),
  h1("2. Work Record"),
  table(["Item", "Student entry"], [
    ["Work start date", "13 / 11 / 2026"], ["Work completion date", "13 / 11 / 2026"],
    ["Main libraries / technologies", "Apache Kafka (KRaft), kafka-python-ng, Spark Structured Streaming (PySpark), psutil, matplotlib, Docker Compose"],
    ["Source files / notebook", "src/config.py, schema.py, broker.py, producer.py, consumer.py, windowing.py, aggregations.py, run_pipeline.py, spark_streaming.py, spark_reference.py, benchmark.py, make_figures.py"],
    ["Test / experiment files", "tests/test_pipeline.py (5 tests, all PASS), src/benchmark.py, output/*.json|*.log|*.txt"],
    ["Final repository commit/version", ""],
  ], [3400, W - 3400], { firstColFill: true }),

  new Paragraph({ children: [new PageBreak()] }),
  h1("Part A — Streaming Architecture"),
  img("architecture.png", 6.6),
  caption("Figure 1. Kafka-based architecture (docs/architecture.png, generated by src/make_figures.py)."),
  table(["Component", "Purpose", "Failure / scale consideration"], [
    ["Producer", "producer.py serialises each event to JSON and publishes it to aitu.research.events, keyed by research_group.", "acks=all + retries=5 so a broker failover does not lose events; idempotence can be enabled to avoid retry duplicates. Scale out by running more producer instances — they are stateless."],
    ["Kafka topic", "aitu.research.events: durable, ordered, replayable log that decouples producers from processors.", "Retention lets a crashed consumer replay from its committed offset. Production RF=3 with min.insync.replicas=2 (dev broker uses RF=1)."],
    ["Partition", "3 partitions; key = research_group, so all events of one group are ordered inside one partition (observed: G01→p2, G02→p1, G03→p0).", "Partitions are the unit of parallelism: max useful consumers = 3. A hot group creates a hot partition (key skew); add partitions/composite keys if groups grow."],
    ["Consumer group", "research-analytics: Kafka assigns each partition to exactly one member, offsets committed per partition.", "If a member dies, the group rebalances and another member resumes from the last committed offset (at-least-once). Commit happens only after the batch is processed."],
    ["Processor", "consumer.py: deserialize → validate schema → dead-letter invalid → running aggregates → 5 s event-time windows with 2 s watermark. Spark job is a second, independent consumer of the same topic.", "Window state is in memory: a restart loses open windows (Spark keeps it in the checkpoint). CPU-bound in Python → scale via more partitions + consumer processes, not threads (GIL)."],
    ["Monitoring", "Consumer lag per partition (Kafka UI on :8080), throughput, p95 latency and CPU from benchmark.py; alerts on lag growth and window failure rate > 20 %.", "Lag that grows steadily is the earliest sign of an under-provisioned processor; it is what revealed the saturation point in Part E."],
  ], [1500, 3900, W - 5400]),

  h1("Part B — Kafka Producer / Consumer"),
  rich("Executed with `python src/run_pipeline.py`: 12 messages consumed (the 10 supplied events plus 2 deliberately invalid test events X001, X002). **10 valid, 2 sent to the dead-letter list.** Valid totals: **completed = " + res.status_counts.completed + ", failed = " + res.status_counts.failed + "** (failure rate 20 %)."),
  table(["Job type", "Completed", "Failed", "Average duration"], partB, [2400, 2400, 2400, W - 7200], { alignRight: true }),
  gap(),
  table(["Research group", "Events", "Average CPU utilization"], cpuG, [3200, 3200, W - 6400], { alignRight: true }),
  gap(),
  para("Schema validation output (from output/pipeline_run.log):", { bold: true }),
  ...code([
    "consumer INVALID p1@3 X001: invalid job_type 'quantum'",
    "consumer INVALID p2@4 X002: field 'duration_sec' has wrong type str",
  ]),
  gap(),

  h1("Part C — Windowed Stream Processing"),
  rich("Tumbling **event-time** windows of 5 s, half-open interval [start, end). window_start = floor(epoch / 5) × 5, so the event at 14:00:05 (E005) belongs to the second window and E010 (14:00:10) to the third. A window is emitted when the watermark (max event time − 2 s) passes its end; the rest are flushed at end of stream."),
  table(["Window", "Events", "Count", "Failure rate", "Average CPU"], win, [2000, 3200, 1100, 1500, W - 7800]),
  gap(),
  img("windows.png", 6.6),
  caption("Figure 2. Window metrics (docs/windows.png)."),
  rich("Check: window 1 = (61+74+38+92)/4 = 66.25 % CPU, 1 failure (E004) of 4 = 25 %; window 2 = (68+81+89+44+94)/5 = 75.20 %, 1 failure (E007) of 5 = 20 %; window 3 = E010 only, 78 %, 0 %."),

  h1("Part D — Spark Structured Streaming"),
  table(["Output field", "Type", "Transformation"], [
    ["window", "struct<start: timestamp, end: timestamp> (printed as HH:mm:ss)", "to_timestamp(timestamp) → withWatermark('event_time', '2 seconds') → window(event_time, '5 seconds')"],
    ["job_type", "string", "groupBy key, after filter job_type ∈ {simulation, ml, image, text} and status ∈ {completed, failed}"],
    ["count", "long", "count('*') per (window, job_type)"],
    ["avg_duration", "double", "round(avg(duration_sec), 2) per (window, job_type)"],
  ], [1700, 3000, W - 4700]),
  gap(),
  para("Core of src/spark_streaming.py:", { bold: true }),
  ...code([
    "events.withWatermark(\"event_time\", \"2 seconds\")",
    "      .groupBy(F.window(\"event_time\", \"5 seconds\"), F.col(\"job_type\"))",
    "      .agg(F.count(\"*\").alias(\"count\"),",
    "           F.round(F.avg(\"duration_sec\"), 2).alias(\"avg_duration\"))",
    "# writeStream: outputMode=update, console sink, trigger 2 s,",
    "# spark.sql.shuffle.partitions=3, checkpointLocation=/tmp/aitu-spark-checkpoint",
  ]),
  gap(),
  para("Expected result for the supplied events (output/spark_reference_output.txt):", { bold: true }),
  table(["window", "job_type", "count", "avg_duration"], sparkRows, [3000, 2400, 1800, W - 7200]),
  para("Note: Spark was not available in the environment where this report was generated, so this table was produced by src/spark_reference.py, a line-by-line Python re-implementation of the same query. Before submitting, run spark-submit (README) and attach the console batch screenshot; the rows must match this table.", { italics: true, size: 18, color: "A33A1A", before: 80 }),

  h1("Part E — Throughput and Latency"),
  rich("Synthetic load from `src/benchmark.py`: constant-rate producer thread (random valid events, 10 groups, 15 % failures), consumer runs the full processor; latency = processed time − produced time (perf_counter_ns stamped in the event). 10 s per level, 2 vCPU machine, in-process broker backend. Final (optimized) consumer:"),
  table(["Input rate (events/s)", "Observed throughput", "Average latency (ms)", "CPU %", "Bottleneck"], perfRows, [1500, 1700, 1900, 1100, W - 6200]),
  gap(),
  para("Extended run — baseline (per-record poll, per-event log line) vs optimized (batch poll of 500, no per-event logging):", { bold: true }),
  table(["Rate", "Base thrpt", "Base avg ms", "Base p95 ms", "Base CPU %", "Opt thrpt", "Opt avg ms", "Opt p95 ms", "Opt CPU %"],
    both, [800, 1100, 1100, 1100, 1050, 1100, 1100, 1100, W - 8450], { alignRight: true }),
  gap(),
  img("performance.png", 6.6),
  caption("Figure 3. Latency and CPU vs input rate (docs/performance.png)."),
  rich(`**Does it scale sufficiently?** Yes for the required range: at 10, 100 and 500 events/s throughput equals input rate and average latency stays below 0.4 ms with ≤ 10 % of one core. Latency stays flat until the consumer approaches one full core. The baseline consumer saturates at 20,000 ev/s (throughput ${b20.throughput_eps} ev/s, average latency ${b20.avg_latency_ms} ms, p95 ${b20.p95_latency_ms} ms, CPU ${b20.cpu_pct} %) — events queue in the partition faster than they are processed, i.e. growing consumer lag. The optimized consumer handles 20,000 ev/s at ${o20.avg_latency_ms} ms and ${o20.cpu_pct} % CPU, 40,000 ev/s at ${ceil[0].avg_latency_ms} ms, and saturates near 58,000 ev/s (${ceil[1].avg_latency_ms} ms average, ${ceil[1].cpu_pct} % CPU).`),
  rich(`**Optimization:** batch processing — poll up to 500 records, process them, commit once per batch, and replace per-event INFO logging with per-window/aggregate logging. At 5,000 ev/s it cut CPU from ${b5k.cpu_pct} % to ${o5k.cpu_pct} %, and it moved the saturation point from below 20,000 ev/s to about 58,000 ev/s. The next step beyond one core is horizontal: more partitions and one consumer process per partition in the same group.`),
  para("Caveat: these measurements use the in-process broker (no network, no disk), so absolute latencies are lower than a real Kafka deployment, where linger.ms, fetch.max.wait.ms and network round-trips add milliseconds. The relative comparison and the bottleneck location (the Python processing loop) still hold. Re-run with --backend kafka for broker-inclusive numbers.", { italics: true, size: 18, color: "52514E" }),

  new Paragraph({ children: [new PageBreak()] }),
  para("STUDENT TECHNICAL REPORT / SUBMISSION RECORD", { size: 28, bold: true, align: AlignmentType.CENTER, after: 200 }),
  h1("3. Report Structure"),

  h2("4.1 Streaming Architecture"),
  evidence([
    ["Technical explanation", "Producers publish keyed JSON events to aitu.research.events (3 partitions). Key = research_group, so per-group order is preserved and the 3 groups map to 3 partitions. The consumer group research-analytics runs the Python processor (validation, aggregates, 5 s windows); Spark Structured Streaming reads the same topic independently. Results go to the aggregation layer (consumer_results.json / Spark sink); monitoring tracks lag, throughput, latency, CPU. Failures: acks=all + retries on the producer, commit-after-process on the consumer (at-least-once), group rebalance on member failure, Spark checkpoint for window state."],
    ["Code / file / function / configuration reference", "docs/architecture.png; src/config.py (TOPIC, NUM_PARTITIONS=3, REPLICATION_FACTOR, CONSUMER_GROUP, PARTITION_KEY); src/broker.py (KafkaProducerWrapper, KafkaConsumerWrapper, MemoryProducer/MemoryConsumer); docker-compose.yml (Kafka KRaft + Kafka UI)."],
    ["Result / calculation / experiment evidence", "output/pipeline_run.log: G01 → partition 2 (offsets 0–3), G02 → partition 1 (0–2), G03 → partition 0 (0–2); consumer reads all 3 partitions, commits after each batch."],
    ["Student-specific decision or observation", "Chose research_group as key (not event_id) so per-group ordering holds and CPU-by-group aggregation can later be done per partition without a shuffle. Trade-off: one heavy group becomes a hot partition."],
  ]),
  gap(),
  h2("4.2 Kafka Producer and Consumer"),
  evidence([
    ["Technical explanation", "Producer loads data/events.json, pins the date (supplied data has only HH:MM:SS), sends each event keyed by research_group. Consumer polls batches, deserialises, validates (required fields, types, job_type/status enums, 0 ≤ cpu_pct ≤ 100, duration ≥ 0, ISO timestamp) and sends invalid records to a dead-letter list instead of stopping the stream; valid events update JobAggregator."],
    ["Code / file / function / configuration reference", "src/producer.py → produce(); src/consumer.py → consume(); src/schema.py → validate(); src/aggregations.py → JobAggregator.add()/report(); tests/test_pipeline.py → test_part_b_aggregates, test_invalid_events_rejected."],
    ["Result / calculation / experiment evidence", "output/consumer_results.json: completed 8, failed 2; avg duration ml 52.33 s, image 77.67 s, text 20.00 s, simulation 107.50 s; avg CPU G01 68.50 %, G02 62.00 %, G03 86.33 %; X001 and X002 rejected."],
    ["Student-specific decision or observation", "Added two malformed events to prove validation works end to end; dead-lettering keeps the pipeline running on bad input. G03 has the highest CPU (86.33 %) and 2 of the 2 failures — failures correlate with CPU > 85 %."],
  ]),
  gap(),
  h2("4.3 Windowed Stream Processing"),
  evidence([
    ["Technical explanation", "Event-time tumbling windows of 5 s, intervals [start, end). Boundary events (14:00:05, 14:00:10) open the next window. Watermark = max event time − 2 s; a window is closed when the watermark passes its end, later events for closed windows are counted as late and dropped. Within a poll, records from different partitions are sorted by event time before windowing."],
    ["Code / file / function / configuration reference", "src/windowing.py → TumblingWindowAggregator.add()/_emit()/flush(), window_start(); config WINDOW_SECONDS=5, WATERMARK_SECONDS=2; tests: test_part_c_windows_boundary_and_values, test_late_event_dropped_after_watermark."],
    ["Result / calculation / experiment evidence", "14:00:00–05: E001–E004, 4 events, 25.0 %, 66.25 %; 14:00:05–10: E005–E009, 5 events, 20.0 %, 75.20 %; 14:00:10–15: E010, 1 event, 0.0 %, 78.00 %. Window 1 closed live when E007 (14:00:07) moved the watermark to 14:00:05."],
    ["Student-specific decision or observation", "2 s lateness is a trade-off: smaller = faster results but more dropped late events from slower partitions; larger = more state and later output."],
  ]),
  gap(),
  h2("4.4 Spark Structured Streaming"),
  evidence([
    ["Technical explanation", "Spark reads Kafka (or JSON-lines files), parses value with an explicit StructType, filters invalid enums, converts timestamp to event_time, applies withWatermark(2 s) and groupBy(window(5 s), job_type) with count and round(avg(duration_sec),2). Update output mode, console sink, 2 s trigger, checkpoint for fault tolerance."],
    ["Code / file / function / configuration reference", "src/spark_streaming.py → build(); spark.sql.shuffle.partitions=3; --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1; src/spark_reference.py (cross-check)."],
    ["Result / calculation / experiment evidence", "9 rows (Part D table); e.g. [14:00:05, 14:00:10) ml count 2, avg 57.50 (51 + 64). Spark console screenshot: ________ (attach after running)."],
    ["Student-specific decision or observation", "Set shuffle partitions to 3 (= Kafka partitions) instead of the default 200, which would create 200 near-empty tasks per micro-batch for this tiny stream."],
  ]),
  gap(),
  h2("4.5 Throughput and Latency"),
  evidence([
    ["Technical explanation", "Constant-rate synthetic producer, full processing path, per-event latency from a perf_counter_ns stamp, process CPU sampled every 0.5 s with psutil. Levels: 10, 100, 500 (required) + 2,000, 5,000, 20,000 and 40,000/60,000 to find saturation."],
    ["Code / file / function / configuration reference", "src/benchmark.py → run_level(); flags --max-poll, --log-each; output/benchmark_baseline.json, benchmark_optimized.json, benchmark_optimized_ceiling.json; docs/performance.png."],
    ["Result / calculation / experiment evidence", `10 → ${opt[0].throughput_eps} ev/s, ${opt[0].avg_latency_ms} ms, ${opt[0].cpu_pct} % CPU; 100 → ${opt[1].throughput_eps} ev/s, ${opt[1].avg_latency_ms} ms, ${opt[1].cpu_pct} %; 500 → ${opt[2].throughput_eps} ev/s, ${opt[2].avg_latency_ms} ms, ${opt[2].cpu_pct} %. Baseline at 20k: ${b20.throughput_eps} ev/s, ${b20.avg_latency_ms} ms.`],
    ["Student-specific decision or observation", "Measured beyond the required 500 ev/s because the required levels never stress the system — the bottleneck only appears above ~5,000 ev/s."],
  ]),
  gap(),
  h2("4.6 Engineering Interpretation"),
  evidence([
    ["Technical explanation", "Main bottleneck: the single-threaded Python processing loop (JSON decode + validation + aggregation + logging) on one core; broker capacity is not the limit at these rates. Under increasing volume, latency stays flat (~0.4 ms) while CPU < ~60 %, then rises sharply once the consumer cannot keep up: lag accumulates and latency grows with queue length (baseline 0.53 ms at 5k → 866 ms at 20k)."],
    ["Code / file / function / configuration reference", "benchmark.py --max-poll 1 --log-each (baseline) vs --max-poll 500 (optimized); consumer.py commits once per batch."],
    ["Result / calculation / experiment evidence", `CPU at 5,000 ev/s: ${b5k.cpu_pct} % → ${o5k.cpu_pct} %; at 20,000 ev/s: ${b20.cpu_pct} % (saturated) → ${o20.cpu_pct} %; saturation moved from < 20,000 to ≈ 58,000 ev/s.`],
    ["Student-specific decision or observation", "Batching is the cheapest fix; beyond one core, scale horizontally (more partitions, one consumer process per partition) because Python threads cannot use more cores under the GIL."],
  ]),

  h1("4. Individual Work and Anti-Plagiarism Record"),
  table(["Individual evidence", "Student entry"], [
    ["My unique technical/design decision", "Partition key = research_group with 3 partitions; Kafka access hidden behind a backend interface (broker.py) so the same producer/consumer/benchmark code runs on real Kafka or on an in-process Kafka emulation with the same partition/offset/consumer-group semantics."],
    ["My own test/experiment", "Baseline vs optimized consumer benchmark at 6 load levels plus a saturation probe (40k/60k ev/s); 5 unit tests for aggregates, window boundaries, validation and late events."],
    ["Problem/bug I personally encountered and solved", "Cross-partition ordering: events of different groups arrive from different partitions out of event-time order, so a naive window could close too early and drop valid events as late. Solved with a watermark (2 s lateness) and sorting each polled batch by event time."],
    ["Exact code/repository/commit reference", "src/windowing.py, src/consumer.py (sort before windowing), src/benchmark.py; commit: ________"],
    ["One limitation of my implementation", "Python processor keeps window state in memory (lost on crash) and benchmark numbers come from the in-process broker, not a networked Kafka cluster."],
    ["One improvement I propose", "Persist window state (or use Spark/Kafka Streams state stores), run one consumer process per partition, and expose lag/latency to Prometheus + Grafana with alert rules."],
  ], [3000, W - 3000], { firstColFill: true }),

  h1("5. Evidence and Submission Checklist"),
  ...["☒ Technical report completed", "☒ Source code / notebook submitted", "☒ Required tables and calculations completed",
    "☒ Required diagrams / plots / dashboard evidence included", "☒ Execution logs or reproducible experiment evidence included",
    "☒ README and run instructions included where required", "☒ Data/schema/configuration files included where required",
    "☒ Performance / analytical results included where required", "☐ Oral defense / demonstration completed"].map((t) => para(t, { after: 40 })),

  h1("6. Student Declaration of Originality"),
  para("I confirm that this submission represents my own work. I have not copied another student's source code, calculations, tables, architecture, plots, screenshots, logs, results, or written explanations. Any external material or reused code has been identified. I understand that the instructor may request a live demonstration, code modification, reproduction of calculations, or explanation of any submitted component."),
  table(["Role", "Name", "Signature", "Date"], [["Student / Executor", "", "", ""], ["Instructor / Accepted by", "", "", ""]], [2800, 2600, 2200, W - 7600]),
];

const doc = new Document({
  styles: {
    default: { document: { run: { font: FONT, size: 20 } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: FONT, size: 30, bold: true, color: "184F95" }, paragraph: { spacing: { before: 280, after: 140 }, keepNext: true, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: FONT, size: 24, bold: true, color: "0B0B0B" }, paragraph: { spacing: { before: 200, after: 100 }, keepNext: true, outlineLevel: 1 } },
    ],
  },
  numbering: { config: [{ reference: "bullets", levels: [{ level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 540, hanging: 270 } } } }] }] },
  sections: [{
    properties: { page: { size: { width: 11906, height: 16838 }, margin: { top: 1134, bottom: 1134, left: 1134, right: 1134 } } },
    footers: { default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER, children: [
      run("Real-time Data Analysis | AITU context | page ", { size: 16, color: "52514E" }),
      new TextRun({ children: [PageNumber.CURRENT], font: FONT, size: 16, color: "52514E" })] })] }) },
    children,
  }],
});
Packer.toBuffer(doc).then((b) => { fs.writeFileSync(path.join(ROOT, "Assignment4_Report.docx"), b); console.log("ok"); });
