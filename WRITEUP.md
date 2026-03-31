# People Counting Pipeline — Technical Write-Up

## 1. Architecture Overview

The pipeline follows a strictly layered architecture where each stage has one 
responsibility and can be tested, debugged, and rerun independently of the others.
```
Raw CSVs (data/)
    │
    ▼
[Phase 1 — Ingest]                          pipeline/ingest.py
Discover CSV files across device folders
Validate and cast column types (TRY_CAST)
Fill missing values with 0, flag them
Sort all rows by (device_id, timestamp)
    │
    ▼
raw_events table (DuckDB, in-memory)
    │
    ▼
[Phase 2 — Transform]                       pipeline/transform.py
Hourly aggregations (DATE_TRUNC + GROUP BY)
Net flow (total_in - total_out per hour)
Occupancy (cumulative SUM window function)
Daily rollup (MAX, MIN, SUM across hours)
    │
    ▼
hourly_aggregations / occupancy / daily_aggregations (DuckDB, in-memory)
    │
    ▼
[Phase 3 — Output]                          pipeline/output.py
Write each table to Parquet
Verify files readable from disk
    │
    ▼
Parquet files (output/)
    │
    ▼
[Django — Load]                             backend/api/management/commands/load_parquet.py
Read Parquet via DuckDB
Bulk insert into SQLite via Django ORM
    │
    ▼
Django REST API (backend/)
JSON endpoints consumed by dashboard
```

A key design principle applied throughout: **design backwards from outputs to 
inputs**. The Django models were fully determinable before a single line of 
pipeline code was written — because the output schema drives everything upstream. 
This avoids the common failure mode of building a pipeline that produces data in 
the wrong shape for its consumer.

---

## 2. Technology Choices

### DuckDB
DuckDB was chosen as the query engine for all pipeline transformations.

The alternatives considered were Pandas and Apache Spark. Pandas loads the entire 
dataset into RAM — it is simple and familiar but does not scale beyond available 
memory. Spark scales to petabyte datasets but requires a JVM runtime, cluster 
configuration, and has significant operational overhead that is inappropriate for 
a two-device dataset or a single-machine deployment.

DuckDB occupies the correct position between them: it processes data using 
columnar storage so queries only read the columns they need, it handles datasets 
larger than RAM through lazy evaluation, it reads Parquet files directly without 
loading them into memory first, and its SQL dialect is standard enough that the 
same queries port to BigQuery or Snowflake with minimal changes if scale eventually 
requires a distributed system.

DuckDB handles datasets larger than available RAM by automatically 
spilling intermediate results to disk when memory pressure is high. 
This degrades performance but prevents crashes — a meaningful 
difference from Pandas, which raises a MemoryError and terminates 
the process when the dataset exceeds available RAM. At the current 
dataset size (25 rows) this distinction is irrelevant. At 100x scale 
it determines whether the pipeline runs slowly or does not run at all.

### Parquet
Pipeline output is stored as Parquet rather than CSV for five specific reasons:

1. **Type preservation** — timestamps remain timestamps, integers remain integers. 
   CSV serialises everything as strings and requires the consumer to guess or 
   re-specify types. This is particularly important for Django, which needs to 
   receive a Python `datetime` object for `DateTimeField` — not a string to parse.

2. **Compression** — Parquet compresses automatically at roughly 5-10x versus 
   equivalent CSV, with no additional configuration.

3. **Columnar reads** — a query selecting only `total_in` reads only that column 
   from disk. CSV requires reading every row in full regardless of how many 
   columns are needed.

4. **Partitioning support** — output can be split by `device_id` and month so 
   that queries for a single device skip all other devices' files entirely. At 
   1,000 devices this is the difference between reading 1/1000th of the data 
   versus all of it.

5. **Industry standard** — Parquet is the dominant format in modern data 
   engineering. It is natively supported by DuckDB, Spark, BigQuery, Snowflake, 
   Redshift, and Pandas without additional dependencies.

### Django + Django REST Framework
Django was specified in the assignment. SQLite was chosen as the backing database 
for simplicity — it requires no server process, no configuration, and is 
appropriate for a single-machine deployment of this scale. In production this 
would be replaced with PostgreSQL, which provides connection pooling, better 
concurrent read performance, and proper indexing for high-traffic API endpoints.

Django REST Framework provides the serialization and routing layer with minimal 
boilerplate. All endpoints are read-only (`ReadOnlyModelViewSet`) because the 
pipeline is the writer and Django is purely a serving layer — enforcing this 
separation at the framework level prevents accidental writes through the API.

### Docker
Docker ensures the pipeline and API run identically on any machine regardless 
of operating system or installed software. A single `docker compose up` command 
builds both containers, runs the test suite, executes the pipeline, loads the 
output into Django's database, and starts the API server. This removes all 
manual environment setup from the deployment process.

---

## 3. Data Model Design

### The Three Output Tables

Three separate output tables were designed with specific query patterns in mind:

| Table | Grain | Primary dashboard use |
|---|---|---|
| `hourly_aggregations` | 1 row per device per hour | Hourly traffic bar chart, net flow line chart |
| `occupancy` | 1 row per device per hour | Occupancy over time chart, current occupancy KPI |
| `daily_aggregations` | 1 row per device per day | Daily KPI cards, weekly trend chart |

**Grain** — what makes each row unique — was defined before any code was written. 
The composite unique constraint in Django models (`unique_together`) enforces this 
at the database level, preventing duplicate rows from being inserted if the loader 
runs multiple times.

### Why Occupancy and Hourly Are Separate Tables

`occupancy` contains almost identical columns to `hourly_aggregations` plus one 
additional column. Keeping them as separate tables was a deliberate decision for 
three reasons:

First, **clarity of purpose**: `hourly_aggregations` answers "what happened each 
hour" (a flow metric). `occupancy` answers "what was the state at each hour" (a 
stock metric). They are different questions even though the underlying data is 
similar. Separate table names communicate intent to anyone querying the data six 
months from now.

Second, **query performance**: the window function computing cumulative occupancy 
is expensive to repeat. Pre-computing it once in the pipeline and storing the 
result means the Django API performs a simple `SELECT` at request time with no 
heavy computation. Running the window function on every API call would make 
dashboard load times degrade as the dataset grows.

Third, **debugging boundary**: if occupancy values appear wrong, the separation 
creates a clean investigation path. Check `hourly_aggregations` first — if 
`net_flow` values are correct, the bug is in the window function logic. If 
`net_flow` is wrong, the bug is in the aggregation. A single combined table 
would require disentangling both possibilities simultaneously.

### Device, Location, and Building Hierarchy

The current model treats `device_id` as the atomic unit and assumes `device_A` 
and `device_B` are independent locations. This is an explicit assumption, not an 
oversight.

In production, multiple sensors are commonly installed at a single location — 
one per entrance, for example. Getting the total occupancy of a building would 
require summing across all devices in that building. This is a standard 
**dimension table** pattern:
```
device_locations table:
device_id  | location_name  | building_id  | floor
device_A   | Main Entrance  | building_1   | 1
device_B   | Side Entrance  | building_1   | 1
```

A `GROUP BY building_id` query joining against this table would produce 
building-level occupancy. The current pipeline schema supports adding this 
without structural changes — `device_id` is present in all output tables and 
the join would be straightforward.

### Schema Contract

The Parquet output schema is the contract between the pipeline and Django. Column 
names and types in the Parquet files must exactly match the Django model field 
definitions. A `test_parquet_schema_matches_contract` test enforces this 
automatically — if a pipeline developer renames a column, the test breaks 
immediately and prevents the mismatch from reaching Django silently.

---

## 4. Data Quality Decisions

The real source data contained several quality issues discovered by inspecting 
the raw CSVs before writing any pipeline code. Each was handled with an explicit 
decision rather than a silent fix.

### Missing Values — Fill and Flag

One approach was to drop rows with missing values entirely. This was rejected 
because dropping rows distorts hourly totals — if the 08:00 hour has three raw 
readings and one is dropped, the aggregated `total_out` for that hour is wrong. 
A second approach was to fail the pipeline and require clean input. This was 
rejected because sensor data is inherently unreliable in production and a pipeline 
that crashes on a single null is operationally fragile.

The chosen approach is to fill missing values with 0 and flag them with 
`has_imputed_in` / `has_imputed_out` boolean columns. This treats a missing value 
as "no activity recorded" — a conservative assumption. The flag column preserves 
the audit trail so downstream consumers can make their own decision: a dashboard 
could display a warning icon on flagged hours, or an analyst could exclude flagged 
rows from a report. Silently filling without flagging would make the imputation 
invisible and unauditable.

### Negative Values — Drop and Report

People counts cannot be negative. A row where `in = -3` represents 
either a sensor firmware bug, data corruption during transmission, or 
a calibration error. Three options were considered: treating negative 
values as nulls and filling with 0 (wrong — it would silently add 
phantom zero-traffic hours), passing them through and letting occupancy 
handle the anomaly (wrong — it corrupts cumulative calculations), or 
dropping the row entirely and reporting it (correct).

Rows with negative `in` or `out` values are dropped during ingestion 
before the cleaned table is created. The count of dropped rows is 
reported in pipeline logs alongside the null count. Both are surfaced 
together because they have the same root cause — unreliable sensor 
hardware — and the same operational response: investigate the specific 
device that produced them.

### Float Values in Integer Columns

If a sensor produces `in = 3.7` (possible if firmware averages readings), 
`TRY_CAST` to INTEGER returns NULL, which is then filled with 0 and 
flagged as imputed. The fractional value is lost. This is acceptable 
for a people-counting context where fractional people are meaningless, 
but the flag ensures the imputation is visible. A production system 
might instead round to the nearest integer before casting, which would 
be a more faithful representation of the original reading.

### Out-of-Order Timestamps

The source data contained rows where an `08:10:00` timestamp appeared after a 
`10:00:00` timestamp in the same file. This would produce incorrect occupancy 
calculations if not corrected — the cumulative window function depends on rows 
being processed in chronological order.

All rows are sorted by `(device_id, timestamp)` during ingestion, before any 
transformation sees the data. The sort happens at the ingestion layer rather than 
the transform layer because ordering is a property of clean data, not a business 
transformation.

### Negative Occupancy — Preserve and Flag

Negative occupancy (more exits recorded than entries) can have several causes: 
sensor miscalibration, tailgating (multiple people exit on one sensor trigger), 
an incorrect starting assumption (the building was not actually empty at time 
zero), or clock drift causing exits to be recorded before their matching entries.

Three approaches were considered. **Clamping to zero** makes the numbers look 
reasonable but destroys the signal — a sensor consistently producing negative 
occupancy is probably broken and needs replacing, but clamping hides that. 
**Rejecting the batch** is appropriate when data quality is contractually 
guaranteed by a vendor, but is too aggressive for sensor data where some 
measurement error is expected. **Preserving and flagging** was chosen: the raw 
cumulative value is kept as-is and an `occupancy_is_invalid` boolean column 
marks affected rows. The dashboard can surface a warning when this flag is true 
while the underlying value remains available for investigation.

### Invalid Timestamps — Drop and Report

Rows with unparseable timestamp values are dropped using `TRY_CAST` rather than 
`CAST`. The regular `CAST` function raises an error and crashes the pipeline on 
the first bad row. `TRY_CAST` returns NULL on failure, allowing the row to be 
filtered out cleanly. The total count of dropped rows is reported in the pipeline 
logs so operators can detect if an unusually high number of rows are being lost — 
which would indicate a systematic format problem rather than isolated noise.

---

## 5. Key Technical Decisions

### Cumulative SUM vs LAG for Occupancy

Occupancy could have been calculated two ways. The first option uses a cumulative 
`SUM(net_flow)` window function — for each row, sum every `net_flow` value from 
the first row up to and including the current row. The second option uses `LAG` 
to retrieve the previous row's occupancy and add the current `net_flow` to it.

The `LAG` approach was rejected for two reasons.

The first is **correctness**: LAG creates a chain dependency. If any single row 
is corrupt or missing, every subsequent row is also wrong because each depends on 
the previous. The cumulative SUM approach recalculates each row independently 
from the source truth (`net_flow`). A corrupt row affects only that row — not 
everything after it. This property is called **idempotency**: running the 
calculation multiple times always produces the same correct result regardless 
of intermediate state.

The second is **performance**: DuckDB uses columnar storage, meaning all values 
of `net_flow` are stored together on disk in a single contiguous block. Summing 
the entire column means reading one sequential block — extremely fast. The LAG 
approach requires retrieving a specific previous row by position, which is a 
comparatively slower random-access pattern in a columnar store.

### Idempotency

The pipeline is designed to be safely re-runnable. Running it twice produces 
the same output as running it once. The `load_parquet` management command 
deletes all existing rows before inserting new ones, ensuring Django's database 
always reflects the latest pipeline output rather than accumulating duplicates. 
DuckDB uses `CREATE OR REPLACE TABLE` for the same reason — each run starts 
from a clean state.

### In-Memory DuckDB

DuckDB runs entirely in-memory during pipeline execution (`:memory:` connection). 
The raw events and intermediate tables are never written to disk — only the final 
Parquet outputs are persisted. This is intentional: intermediate tables are 
pipeline artefacts, not deliverables. Persisting them would create stale data 
that could be confused with current output on a re-run.

---

## 6. Scalability Analysis

The current dataset is intentionally small (25 rows across 4 files). The 
pipeline is designed with explicit awareness of what would need to change at 
larger scale.

### Batch Processing

The pipeline is a **batch** system: events accumulate in CSV files and are 
processed together at scheduled intervals (e.g. nightly). This is the correct 
model for this use case — a people-counting dashboard does not require real-time 
updates. Streaming processing (events processed individually as they arrive) 
would add significant infrastructure complexity for no practical benefit at this 
latency requirement.

### What Breaks at 100x Scale

| Component | Current behaviour | Problem at 100x | Solution |
|---|---|---|---|
| Ingestion | All files loaded into RAM | Memory exhaustion | DuckDB lazy evaluation — process files in batches without full materialisation |
| Occupancy calculation | Full recalculation from row 1 every run | Grows linearly with history | Incremental checkpointing (see below) |
| Parquet output | Single file per table | Large files, slow queries | Partition by `device_id` and month — queries skip irrelevant partitions |
| Django database | SQLite, no connection pooling | Bottleneck under concurrent API load | Migrate to PostgreSQL with connection pooling and composite indexes |
| Pipeline scheduling | Manual or single Docker run | No retry, no alerting, no history | Orchestration with Airflow or Prefect |

### Incremental Checkpointing

The current pipeline recalculates occupancy from row one on every run. 
At small scale this is fine. At large scale — years of data across 
thousands of devices — the amount of work grows indefinitely even 
though only one new day of data arrives each night.

The production solution is incremental checkpointing. At the end of 
each pipeline run, the ending occupancy per device is saved to a 
checkpoint file:
```json
{
    "device_A": {
        "last_processed_date": "2024-01-01",
        "ending_occupancy": 0,
        "run_timestamp": "2024-01-02T00:00:00"
    },
    "device_B": {
        "last_processed_date": "2024-01-01",
        "ending_occupancy": 12,
        "run_timestamp": "2024-01-02T00:00:00"
    }
}
```

The next run loads this checkpoint and processes only new files:
```
Run 1 (Jan 1 data):
  No checkpoint → process all data
  Save checkpoint: {device_A: ending_occupancy=0, device_B: ending_occupancy=12}

Run 2 (Jan 2 data arrives):
  Load checkpoint → device_A starts at 0, device_B starts at 12
  Process ONLY Jan 2 files
  Window function starts from checkpoint values, not zero
  Save new checkpoint: {device_A: 8, device_B: 16}

Run N: constant work regardless of total historical data size
```

This only changes the ingestion layer — the transform and output logic 
stays identical. The checkpoint value is injected as a starting offset 
before the window function runs. LAG would not work here because it 
has no memory between separate pipeline runs — the checkpoint file 
provides that cross-run memory.

This was not implemented in the current pipeline because with two days 
of data the performance difference is zero and the added complexity 
would obscure the core pipeline logic.

### Granularity

Hourly granularity was chosen as the default. The `DATE_TRUNC` function that 
controls this is parameterised — changing to 15-minute or daily buckets requires 
one configuration change. 

A general rule applies: the pipeline should store the finest granularity 
justified by the use case, because you can always aggregate finer to coarser 
at query time, but you cannot reconstruct finer granularity from coarser data 
once it has been discarded. Heavy aggregation belongs in the pipeline; 
light filtering belongs in the dashboard.

---

## 7. Production Gaps

The following are intentionally absent from this implementation. Each represents 
a real production concern.

**Orchestration** — the pipeline runs once manually or via Docker. A production 
system would schedule runs with Airflow or Prefect, with dependency tracking, 
automatic retries on failure, alerting when a run fails or takes longer than 
expected, and a visual DAG of pipeline stages.

**Schema evolution** — if a sensor vendor changes the CSV format (adds columns, 
renames fields, changes timestamp format), the pipeline would produce wrong output 
silently. Production systems use schema registries or explicit schema validation 
at the ingestion boundary to detect and handle format changes explicitly.

**Late-arriving data** — if a sensor goes offline and uploads three days of 
backlogged events today, the current pipeline will not reprocess the affected 
days' aggregations. A production system needs a reprocessing strategy: detect 
which historical partitions are affected by late data and re-run only those.

**Monitoring and alerting** — there is no alerting if row counts drop 
unexpectedly (sensor offline), if a device stops appearing in the data, if 
occupancy values are anomalous, or if pipeline run time increases significantly. 
Production pipelines emit metrics to a monitoring system such as Datadog or 
Prometheus and page on-call engineers when thresholds are exceeded.

**Authentication** — the REST API has no authentication. In production, endpoints 
would be protected with API keys or OAuth tokens. The Django admin interface 
would be disabled or restricted on the public network.

**Production web server** — Django's development server (`manage.py runserver`) 
is single-threaded and not designed for production traffic. A production 
deployment would use Gunicorn or uWSGI behind an Nginx reverse proxy.

**Bronze layer** — the current pipeline reads raw CSVs and cleans them 
in a single ingestion step. A production system would preserve a bronze 
layer — an immutable copy of raw data exactly as received — before any 
cleaning or transformation. Our data/ folder serves this purpose 
informally (the pipeline never modifies source files), but a formal 
bronze layer would store timestamped copies of every file received, 
enabling full reprocessing from the original source if cleaning logic 
needs to change.

**Drift correction** — occupancy is derived by accumulating sensor 
flow measurements over time. Any measurement error — a missed exit, 
a null filled with 0, a miscalibrated sensor — compounds permanently 
into the cumulative sum. This is called drift, and it is an inherent 
risk in any system that derives state from accumulated flow data.

In production, drift is typically discovered when a dashboard user 
notices occupancy values that are physically implausible (e.g. 847 
people in a 200-person building). By that point the error may have 
accumulated across several days of pipeline runs.

The correction workflow has two parts. First, a corrections file 
provides ground truth occupancy values at known points in time — 
typically taken from a manual count or a security system reset:
```csv
timestamp,           device_id, actual_occupancy
2024-01-02T09:00:00, device_A,  0
2024-01-03T09:00:00, device_A,  0
```

Second, the occupancy window function restarts its accumulation from 
the most recent correction value before each row, rather than always 
starting from zero at the beginning of the dataset. Only the transform 
layer changes — ingestion and output are unaffected.

This is a standard **backfill** operation in production data 
engineering: reprocessing historical data with corrected inputs, 
rewriting the affected Parquet partitions, and reloading Django's 
database. The correction is not instantaneous — it takes effect on 
the next pipeline run, which is acceptable in a batch system where 
nightly updates are the norm.

If immediate correction is required, a Django management command can 
patch the occupancy values directly in the serving database as a 
temporary measure, with the understanding that the pipeline's next 
run will overwrite it unless the source correction is also applied.

For locations with guaranteed empty periods (retail stores, offices), 
a simpler alternative is a daily partition reset — changing 
`PARTITION BY device_id` to `PARTITION BY device_id, DATE_TRUNC('day', hour)` 
in the occupancy window function. One line change that eliminates 
drift entirely for predictable operating patterns.
---

## 8. Assumptions

The following assumptions were made explicitly and documented here rather than 
embedded silently in the code:

- **Starting occupancy is 0** for all devices at the beginning of the dataset. 
  If the building was not actually empty at the first recorded timestamp, 
  occupancy values will be offset by the unknown starting count.

- **device_A and device_B are independent locations**. The data does not 
  indicate whether they are entrances to the same building or different buildings. 
  They are treated as independent. A device-to-location dimension table would 
  resolve this ambiguity in production.

- **Missing sensor values represent no activity**, not sensor failure. Filling 
  with 0 is the conservative choice. If missing values more often represent sensor 
  failure (no reading taken) than genuine zero traffic, a different strategy — 
  such as linear interpolation from neighbouring values — would be more appropriate.

- **Hourly granularity is appropriate** for this use case. Different deployment 
  contexts (retail, stadium, corporate office) may require finer or coarser 
  granularity.

- **The dataset covers only two days** intentionally. The pipeline is designed 
  for this scale but the architecture decisions (partitioning strategy, 
  incremental checkpointing, parameterised granularity) anticipate the 
  requirements of a larger production dataset.