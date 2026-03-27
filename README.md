# People Counting Pipeline

A data pipeline that processes raw sensor event data into aggregated 
analytics datasets, served via a REST API designed for dashboard consumption.

Built as a data engineering exercise using DuckDB, Parquet, Django, 
and Docker.

---

## Architecture
```
Raw CSVs (data/)
    │
    ▼
[Phase 1 — Ingest]
Discover CSV files, validate types,
fill nulls, sort timestamps
    │
    ▼
[Phase 2 — Transform]
Hourly aggregations → Net flow → Occupancy (window function) → Daily rollup
    │
    ▼
[Phase 3 — Output]
Write Parquet files (output/)
    │
    ▼
[Django REST API]
Load Parquet → SQLite → Serve JSON endpoints
    │
    ▼
Dashboard-ready JSON API
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop) 
  (only requirement to run the full stack)

For local development without Docker:
- Python 3.10+
- pip

---

## Quickstart (Docker)

This runs the full pipeline and starts the API server in one command:
```bash
docker compose up
```

What happens:
1. Pipeline container runs 18 tests — if any fail, pipeline stops
2. Pipeline processes raw CSVs and writes Parquet files to `output/`
3. Django container loads Parquet files into its database
4. API server starts at http://localhost:8000

To stop everything:
```bash
Ctrl+C
docker compose down
```

---

## Project Structure
```
people-counter/
├── data/                        # Raw input CSVs (sensor event data)
│   ├── device_A/
│   │   ├── 2024-01-01.csv
│   │   └── 2024-01-02.csv
│   └── device_B/
│       ├── 2024-01-01.csv
│       └── 2024-01-02.csv
│
├── pipeline/                    # Core pipeline code
│   ├── ingest.py                # Phase 1: reads CSVs into DuckDB
│   ├── transform.py             # Phase 2: aggregations and occupancy
│   └── output.py                # Phase 3: writes Parquet files
│
├── output/                      # Generated Parquet files (git-ignored)
│   ├── hourly/
│   ├── occupancy/
│   └── daily/
│
├── tests/                       # pytest test suite (18 tests)
│   ├── conftest.py              # Shared fixtures
│   ├── test_ingest.py
│   ├── test_transform.py
│   └── test_output.py
│
├── backend/                     # Django REST API
│   ├── api/
│   │   ├── models.py            # Database table definitions
│   │   ├── serializers.py       # JSON serialization
│   │   ├── views.py             # API endpoint logic
│   │   └── management/
│   │       └── commands/
│   │           └── load_parquet.py  # Loads Parquet into Django DB
│   └── config/
│       ├── settings.py
│       └── urls.py
│
├── run_pipeline.py              # Pipeline entry point
├── Dockerfile                   # Pipeline container definition
├── backend/Dockerfile.backend   # Django container definition
├── docker-compose.yml           # Orchestrates both containers
└── WRITEUP.md                   # Design decisions and trade-offs
```

---

## API Reference

Base URL: `http://localhost:8000`

All endpoints are read-only (GET). No authentication required.

### GET /api/devices/
Returns all known device IDs. Use this to populate a device selector.
```json
["device_A", "device_B"]
```

---

### GET /api/hourly/
Returns hourly traffic aggregations.

**Query parameters:**
| Parameter | Type | Description |
|---|---|---|
| `device_id` | string | Filter by device (e.g. `device_A`) |
| `date` | date | Filter by date (e.g. `2024-01-01`) |

**Example:** `GET /api/hourly/?device_id=device_A&date=2024-01-01`
```json
[
  {
    "device_id": "device_A",
    "hour": "2024-01-01T08:00:00Z",
    "total_in": 13,
    "total_out": 1,
    "net_flow": 12,
    "has_imputed_in": false,
    "has_imputed_out": true
  }
]
```

**Fields:**
- `total_in` / `total_out` — people entering/exiting that hour
- `net_flow` — `total_in - total_out` (negative means more exits than entries)
- `has_imputed_in` / `has_imputed_out` — true if any null was filled with 0

---

### GET /api/occupancy/
Returns running occupancy per device per hour.

**Query parameters:**
| Parameter | Type | Description |
|---|---|---|
| `device_id` | string | Filter by device |
| `date` | date | Filter by date |

**Example:** `GET /api/occupancy/?device_id=device_A&date=2024-01-01`
```json
[
  {
    "device_id": "device_A",
    "hour": "2024-01-01T08:00:00Z",
    "occupancy": 12,
    "occupancy_is_invalid": false,
    "has_imputed_out": true
  }
]
```

**Fields:**
- `occupancy` — cumulative people count at that hour (assumes start = 0)
- `occupancy_is_invalid` — true if occupancy went negative (sensor issue)

---

### GET /api/daily/
Returns daily summaries per device.

**Query parameters:**
| Parameter | Type | Description |
|---|---|---|
| `device_id` | string | Filter by device |

**Example:** `GET /api/daily/?device_id=device_A`
```json
[
  {
    "device_id": "device_A",
    "date": "2024-01-01",
    "total_in": 20,
    "total_out": 20,
    "net_flow": 0,
    "peak_occupancy": 14,
    "min_occupancy": 0
  }
]
```

**Fields:**
- `peak_occupancy` — highest occupancy recorded that day
- `min_occupancy` — lowest occupancy recorded that day

---

## API → Dashboard Mapping

| Dashboard view | Endpoint | Notes |
|---|---|---|
| Device selector dropdown | `GET /api/devices/` | Call once on page load |
| Hourly traffic bar chart | `GET /api/hourly/?device_id=X` | x-axis: hour, y-axis: total_in / total_out |
| Net flow line chart | `GET /api/hourly/?device_id=X` | y-axis: net_flow |
| Occupancy over time | `GET /api/occupancy/?device_id=X&date=Y` | y-axis: occupancy |
| Daily KPI cards | `GET /api/daily/?device_id=X` | peak_occupancy, total_in |
| Data quality warnings | Any endpoint | Filter where has_imputed_out = true or occupancy_is_invalid = true |

---

## Running Tests

### With Docker
Tests run automatically before the pipeline on every `docker compose up`.

### Locally
```bash
# From project root
pytest tests/ -v
```

Expected output: 18 passed

---

## Local Development (without Docker)
```bash
# 1. Create and activate virtual environment
python -m venv .venv
.venv\Scripts\Activate.ps1        # Windows
source .venv/bin/activate          # Mac/Linux

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the pipeline
python run_pipeline.py

# 4. Start the Django API
cd backend
pip install -r requirements.txt
python manage.py migrate
python manage.py load_parquet --output-dir ../output
python manage.py runserver
```

API available at http://localhost:8000

---

## Data Quality Notes

The source data contains intentional quality issues that the pipeline handles:

| Issue | How handled |
|---|---|
| Missing `out` values | Filled with 0, flagged in `has_imputed_out` |
| Out-of-order timestamps | Sorted by `(device_id, timestamp)` during ingestion |
| Unparseable timestamps | Row dropped, count reported in pipeline logs |
| Negative occupancy | Preserved as-is, flagged in `occupancy_is_invalid` |

---

## Design Decisions and Trade-offs

See [WRITEUP.md](./WRITEUP.md) for a full explanation of architecture 
decisions, scalability analysis, and production considerations.