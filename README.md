# Healthcare Lakehouse with Trino & Avro

A modern healthcare data lakehouse demo that streams synthetic patient vitals via Kafka, persists clinical data in MySQL, and federates queries across all layers using Trino into an Iceberg/MinIO lakehouse with Apache Superset visualization.

---

## Quick Start (the whole demo in ~3 minutes)

> This is the only path you need to follow. Commands are copy-paste safe and
> can be re-run safely.

```bash
# 0. Environment + Python deps (one time)
cp .env.example .env
make setup

# 1. Start the stack (10 services; wait ~30s for everything to go healthy)
make up
make check            # keep going once the three required services show [OK]

# 2. Load the 15 clinical tables into MySQL + apply the Kafka enrichment
make seed

# 3. Copy MySQL data into Iceberg/Nessie (MinIO), then validate parity
make migrate          # prints Iceberg vs MySQL counts at the end
make validate         # optional: re-run the parity check

# 4. Start the live vitals stream (leave this running in its own terminal)
make produce

# 5. Run the federated demo (Kafka <> MySQL <> Iceberg in one query)
docker exec -i trino trino < demo_script/demo_script.sql
```

If step 5 prints patient names next to heart-rate values, the pipeline works.

**What just happened, in one sentence per step:**
`make seed` loads the patient/encounter/observation data and adds a `patient_id`
column so Kafka records can be matched; `make migrate` snapshots those tables as
AVRO Iceberg tables in MinIO (versioned by Nessie); `make produce` streams Avro
vitals (validated against the Schema Registry) into the `telemetry.vitals`
topic; and `demo_script.sql` joins the live stream against MySQL then freezes
high-heart-rate rows into an Iceberg archive table.

---

## Architecture

This demo has **two distinct data paths** that share the same Trino query engine.
Understanding the difference is key.

```
                          ┌──────────────────────────────────────────────────┐
                          │              TRINO (query engine)                │
                          │    federated queries across all sources at once   │
                          └───┬───────────────┬──────────────────┬───────────┘
                              │               │                  │
                  ┌───────────▼──┐   ┌────────▼────────┐   ┌───▼───────────┐
                  │    MySQL 8   │   │      Kafka       │   │ Iceberg/Nessie│
                  │ (source of  │   │ (live vitals     │   │ (persistent   │
                  │  truth for  │   │  stream, Avro    │   │  lakehouse    │
                  │  clinical   │   │  + Schema Reg.)  │   │  in MinIO)    │
                  │  data)      │   │                  │   │               │
                  └──────┬──────┘   └──────┬───────────┘   └───────────────┘
                         │                 │
                  Synthea CSV          Python producer
                  (seeded via          (make produce)
                   make seed)
```

### Path 1: Batch — Clinical data (MySQL → Iceberg)

The clinical data is static — it doesn't change at runtime. It's loaded once
from Synthea-generated CSV files into MySQL (15 tables, ~250,000 records), then
**copied** into Iceberg/MinIO via Trino so it lives in an open, queryable
lakehouse format:

```
Synthea CSV → make seed (MySQL) → make migrate (Trino → Iceberg/Nessie/MinIO)
```

- **MySQL** holds the authoritative copy (`mysql.healthcare.*`).
- **Iceberg** is a second copy in open format (`nessie.healthcare.*`), versioned
  by Nessie so you can roll back or branch like Git.
- `make validate` checks that the two copies have identical row counts.

### Path 2: Streaming — Live vitals (Kafka)

The vitals producer runs as a long-lived process, generating ~1 synthetic
vitals message per second into Kafka's `telemetry.vitals` topic. Each message
is Avro-encoded and validated against the Confluent Schema Registry before
it's published.

```
make produce → Avro + Schema Registry → Kafka topic "telemetry.vitals"
```

- **Kafka retains messages for a limited window** (default ~7 days). After
  that, messages are deleted automatically. The data is real but ephemeral.
- Trino reads the topic **live at query time** — it does not copy the data
  anywhere. Each query decodes the latest messages from Kafka's log.
- Schema Registry ensures every message conforms to the Avro schema (string
  `patient_id`, int `heart_rate`, etc.), so Trino always sees consistent columns.

### Where they meet: The federated join

The demo's key trick is a **single SQL query that spans both paths**:

```sql
SELECT p.first, v.heart_rate, from_unixtime(v.timestamp / 1000)
FROM kafka.default."telemetry.vitals" v          -- live stream (ephemeral)
JOIN mysql.healthcare.patients p ON v.patient_id = p.patient_id  -- clinical (permanent)
```

Trino executes this as one query across two completely separate systems:
it reads the Kafka topic in real time while looking up patient names in
MySQL. No ETL, no data movement — just SQL.

### Why the Iceberg archive exists

Because Kafka data is temporary, the demo archives the interesting subset
(high heart rate) into an Iceberg table:

```
federated join WHERE heart_rate > 100 → nessie.clinical_archive.high_vitals_history
```

This is the **"freeze"** step: a live-stream query result is persisted into
the lakehouse as an Iceberg table, where it's queryable forever and versioned
by Nessie. In a real system, this is how you'd build a historical analytics
layer on top of a streaming pipeline.

### Components

| Service | Port | Role |
|---------|------|------|
| MySQL 8.0 | 3306 | Source clinical database (patients, encounters, medications, observations, etc.) |
| Kafka | 9092/29092 | Streaming platform — holds the live vitals topic |
| Schema Registry | 8081 | Avro schema validation for Kafka messages |
| Trino | 8082 | Distributed SQL — federates queries across MySQL, Kafka, and Iceberg |
| Nessie | 19120 | Git-like catalog for Iceberg schema versioning |
| MinIO | 9000/9001 | S3-compatible object storage (Iceberg tables live here) |
| Apache Superset | 8088 | Data visualization and dashboards |
| Redpanda Console | 8080 | Web UI for browsing Kafka topics and messages |

## Prerequisites

- Docker and Docker Compose (Docker Desktop on macOS/Windows)
- Python 3.12+
- uv (recommended) or pip

## Details per Quick Start step

### 1. Start the infrastructure

```bash
make up
# Or without make:
docker compose up -d
```

Wait for all services to become healthy, then verify:

```bash
make check
# Or: uv run check_stock.py
```

### 2. Seed the database

`make seed` loads the 15 Synthea clinical tables into MySQL and then applies
`scripts/migration/V2__enrich_patients_for_kafka.sql`, which adds the `patient_id`
column and maps 8 patients to the `P####` IDs used by the vitals producer.

```bash
make seed
```

### 3. Run the migration into Iceberg

`make migrate` runs the Trino scripts in `scripts/migration/` that copy the MySQL
tables into AVRO Iceberg tables in Nessie/MinIO, then runs the parity check.

```bash
make migrate
```

The final lines should show matching counts, e.g.
`("MySQL Count","80751","Iceberg Count","80751")`.

### 4. Run the vitals producer

```bash
make produce
# Or: uv run produce_vitals.py
```

The producer serializes records with Avro, validates them against the Confluent
Schema Registry, and sends them to the `telemetry.vitals` Kafka topic. Patient
IDs are drawn from the set populated by `make seed` (e.g. `P1001`), so every
record joins to a row in the MySQL `patients` table. Trino auto-discovers the
Avro table from the Schema Registry subject via the Kafka connector's CONFLUENT
supplier.

> For a plain-JSON variant without Schema Registry, run `make produce-json`
> (`uv run producer.py`) — it publishes to the separate `vitals` topic. Querying
> that topic still works, but the demo below targets the Avro table.

### 5. Run the federated demo

```bash
docker exec -i trino trino < demo_script/demo_script.sql
```

This query joins the **live Kafka stream** with **MySQL patients**, then archives
the high-heart-rate rows into the **Iceberg** table
`nessie.clinical_archive.high_vitals_history`. The same join, ad hoc:

```sql
-- Federated join: live Kafka vitals + MySQL patients
SELECT
    p.first AS name,
    p.last AS surname,
    v.heart_rate,
    v.temperature,
    from_unixtime(v.timestamp / 1000) AS last_update
FROM kafka.default."telemetry.vitals" v
JOIN mysql.healthcare.patients p ON v.patient_id = p.patient_id
WHERE v.heart_rate > 100
ORDER BY v.timestamp DESC
LIMIT 5;
```

Open the Trino UI at http://localhost:8082 to run queries interactively.

### 6. Visualize in Superset

Superset connects to Trino and the dashboards are provisioned automatically:

```bash
make superset-setup
# Or: uv run python scripts/setup_superset.py   (idempotent, safe to re-run)
```

The script creates the `Trino (Iceberg)` database connection
(`trino://trino@trino:8080/nessie`), four datasets (patients, encounters,
observations, high heart-rate archive), four example charts, and the
`Healthcare Lakehouse` dashboard, then verifies each chart returns rows.

Then open http://localhost:8088 and log in with the admin credentials from
`.env` (default `admin`/`admin`).

### 7. Learn SQL on the live stack

A four-part tutorial series (`tutorials/`) teaches time-series SQL, CTEs, and
window functions against the real data, framed as business questions an analyst
would receive. Every query executes live against the federated stack.

```bash
make tutorial TUT=01   # time-series basics & CTEs
make tutorial TUT=02   # window functions (row_number, lag, rolling averages)
make tutorial TUT=03   # monthly buckets, running totals, year-over-year
make tutorial TUT=04   # federated real-time alerts (Kafka + MySQL + Iceberg)
```

See `tutorials/README.md` for the data model, the learning path, and Superset
exercises.

## Available Make Targets

```bash
make help          # Show all available targets
make up            # Start all services
make down          # Stop all services
make status        # Show service status
make check         # Run health checks
make seed          # Seed MySQL (15 tables) + apply V2 enrichment
make produce        # Start Avro vitals producer (primary, telemetry.vitals topic)
make produce-json   # Start JSON vitals producer (alternative, vitals topic)
make docker-produce # Start Avro vitals producer against the compose stack
make migrate       # MySQL -> Iceberg (AVRO) in Nessie/MinIO + parity check
make validate      # Re-run the data parity check
make superset-setup # Provision Superset dashboards (idempotent)
make tutorial       # Run a SQL tutorial (usage: make tutorial TUT=01)
make test          # Run unit tests
make lint          # Run linter and formatter
make clean         # Remove containers, volumes, and data
```

## Project Structure

```
.
├── docker-compose.yaml          # Service orchestration
├── pyproject.toml               # Python project config
├── Makefile                     # Common operations
├── .env.example                 # Environment variable template
├── produce_vitals.py            # Kafka vitals producer (Avro, local)
├── docker_produce_vitals.py     # Kafka vitals producer (Avro, Docker)
├── producer.py                  # Kafka vitals producer (JSON, alternative)
├── check_stock.py               # Stack health checker
├── sql/                         # MySQL seed data (15 tables)
├── scripts/migration/           # Trino migration SQL
├── demo_script/                 # Demo queries
├── trino/catalog-templates/     # Trino catalog config templates (rendered via .env)
├── docker/trino-bootstrap.sh    # Renders catalog templates at container start
└── tests/                       # Unit tests
```

## Data Model

The healthcare database contains 15 clinical tables based on the Synthea open dataset:

| Table | Description |
|-------|-------------|
| patients | Patient demographics and demographics |
| encounters | Healthcare encounters/visits |
| conditions | Diagnosed conditions |
| medications | Prescribed medications |
| procedures | Medical procedures performed |
| observations | Clinical observations (vitals, labs) |
| allergies | Patient allergies |
| careplans | Care plan orders |
| devices | Medical devices used |
| imaging_studies | Imaging study records |
| immunizations | Vaccination records |
| organizations | Healthcare organizations |
| payers | Insurance payers |
| payer_transitions | Insurance coverage changes |
| providers | Healthcare providers |

## Kafka Topics

| Topic | Format | Description |
|-------|--------|-------------|
| `telemetry.vitals` | Avro | Real-time patient vitals (primary demo stream; Schema Registry, joins MySQL by `patient_id` string) |
| `vitals` | JSON | Plain-JSON alternative (no Schema Registry; `make produce-json`) |

## Development

### Running tests

```bash
make test           # Run all tests
pytest tests/ -v    # Verbose output
pytest tests/ -m unit    # Unit tests only
```

### Linting and formatting

```bash
make lint           # Run ruff check + format
ruff check .        # Check only
ruff format .       # Auto-format
mypy .              # Type checking
```

### Adding a new migration

- **Trino/Iceberg migrations** (e.g. `04_my_change.sql`): create with a numeric
  prefix in `scripts/migration/`, then run `make migrate`. The `make migrate`
  loop picks them up automatically. Keep them idempotent (`CREATE TABLE IF NOT
  EXISTS`) so re-runs are safe.
- **MySQL migrations** (e.g. `V3__.sql`): run via docker exec against the `db`
  service, e.g. `docker exec -i db mysql -u root -prootpassword healthcare <
  scripts/migration/V3__.sql`. These are NOT part of `make migrate`.

## Troubleshooting

### Services won't start

```bash
# Check service status
docker compose ps

# View logs
docker compose logs -f [service-name]

# Check for port conflicts
lsof -i :3306 -i :9092 -i :8081 -i :8082 -i :19120
```

### Trino can't connect to MySQL

- Ensure MySQL healthcheck passes: `docker compose ps db`
- Wait for MySQL to be fully initialized (first run takes ~30s)
- Check Trino MySQL catalog: `trino/catalog-templates/mysql.properties.tmpl`

### Kafka topic not found

- Ensure Kafka is healthy: `docker compose ps kafka`
- The `telemetry.vitals` topic is created automatically by the Avro producer (and
  its Schema Registry subject) on first send. To recreate a clean topic after a
  schema change, delete the topic and its subject:
  ```bash
  docker exec kafka kafka-topics --bootstrap-server localhost:29092 \
    --delete --topic telemetry.vitals
  docker exec schema-registry curl -X DELETE "http://localhost:8081/subjects/telemetry.vitals-value?permanent=true"
  ```
- The JSON `vitals` topic is created automatically by `producer.py` on first send.

### Federated join returns no rows

- Causes: the `V2__enrich_patients_for_kafka` migration hasn't run (so MySQL
  `patient_id` is NULL), or the producer hasn't sent any records yet.
- Fixes:
  ```bash
  make seed    # re-applies the V2 enrichment (idempotent)
  make produce # then re-run the demo query
  ```
- Verify the mapping landed: `SELECT patient_id, first, last FROM
  mysql.healthcare.patients WHERE patient_id IS NOT NULL;`

### MinIO bucket missing

- The `createbuckets` init container should create `icebergwarehouse` automatically
- Manually create via MinIO console at http://localhost:9001

### Superset shows no databases

- Ensure Trino is running and healthy
- Add Trino connection in Superset: Settings > Data > Databases > + > Trino
- Connection string: `trino://trino@trino:8080/nessie`

### Superset SQL Lab errors: "Column 'record_count' cannot be resolved"

- **Cause:** the bundled Trino SQLAlchemy driver (`trino==0.339.0`) mistakes the
  aggregate columns of an *unpartitioned* Iceberg `$partitions` table
  (`record_count`, `file_count`, `total_size`, `data`) for real partition
  columns. Superset then auto-runs bogus preview queries like
  `WHERE record_count = 80751`.
- **Fix (automatic):** the Superset bootstrap applies a small vendored patch to
  the dialect on every container start (see `docker/patches/trino_iceberg_partitions.py`).
  If you still see the error, close SQL Lab tabs and hard-refresh so the stale
  preview state is cleared.
- To drop this workaround when upgrading the driver, keep
  `docker/requirements-local.txt` in sync and remove the patch step from
  `docker/superset-bootstrap.sh` once the upstream dialect is fixed.

### Superset SQL Lab rejects INSERT/CREATE

- The Trino database connection has DML disabled (`allow_dml = false`) on
  purpose. Enable it (Settings > Databases, or `PUT /api/v1/database/<id>`) if
  you need to run DDL/DML from SQL Lab.

## Security Notes

- All credentials in this demo are for **local development only**
- Never commit real passwords to version control
- Use environment variables (`.env`) for all secrets
- Bind ports to `127.0.0.1` in production

## License

This project is for demonstration and educational purposes.
