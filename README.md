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
make check            # keep going once every line is [OK]

# 2. Load the 15 clinical tables into MySQL + apply the Kafka enrichment
make seed

# 3. Copy MySQL data into Iceberg/Nessie (MinIO), then validate parity
make migrate          # prints Iceberg vs MySQL counts at the end
make validate         # optional: re-run the parity check

# 4. Start the live vitals stream (leave this running in its own terminal)
uv run producer.py

# 5. Run the federated demo (Kafka <> MySQL <> Iceberg in one query)
docker exec -i trino trino < demo_script/demo_script.sql
```

If step 5 prints patient names next to heart-rate values, the pipeline works.

**What just happened, in one sentence per step:**
`make seed` loads the patient/encounter/observation data and adds a `patient_id`
column so Kafka records can be matched; `make migrate` snapshots those tables as
AVRO Iceberg tables in MinIO (versioned by Nessie); `producer.py` streams JSON
vitals into the `vitals` Kafka topic; and `demo_script.sql` joins the live stream
against MySQL then freezes high-heart-rate rows into an Iceberg archive table.

---

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌───────────────┐
│   MySQL 8   │────▶│    Trino     │────▶│   Superset    │
│  (Clinical) │     │  (Federated  │     │(Visualization)│
└─────────────┘     │   Queries)   │     └───────────────┘
                    └──────┬───────┘
                           │
┌─────────────┐     ┌──────▼───────┐     ┌───────────────┐
│    Kafka    │────▶│     Nessie   │────▶│     MinIO     │
│   (vitals)  │     │  + Iceberg   │     │  (S3 Storage) │
└─────────────┘     └──────────────┘     └───────────────┘
```

### Components

| Service | Port | Purpose |
|---------|------|---------|
| MySQL 8.0 | 3306 | Source clinical database (patients, encounters, medications, etc.) |
| Nessie | 19120 | Git-like catalog for Iceberg schema versioning |
| MinIO | 9000/9001 | S3-compatible object storage for Iceberg tables |
| Kafka | 9092/29092 | Streaming platform for real-time patient vitals |
| Schema Registry | 8081 | Avro schema management for Kafka topics |
| Trino | 8082 | Distributed SQL query engine for federated queries |
| Redpanda Console | 8080 | Web UI for Kafka topics and messages |
| Apache Superset | 8088 | Data visualization and dashboards |

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
# Or: uv run producer.py
```

The producer sends JSON records to the `vitals` Kafka topic. Patient IDs are
drawn from the set populated by `make seed`, so every record joins to a row in
the MySQL `patients` table.

> For the Avro / Schema Registry variant, run `uv run produce_vitals.py` — it
> publishes to the separate `telemetry.vitals` topic (integer patient IDs). Trino
> cannot decode those messages through the JSON `kafka.default.vitals` table, so
> the federated demo below uses the JSON topic.

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
    from_unixtime(v.timestamp / 1000) AS last_update
FROM kafka.default.vitals v
JOIN mysql.healthcare.patients p ON v.patient_id = p.patient_id
WHERE v.heart_rate > 100
ORDER BY v.timestamp DESC
LIMIT 5;
```

Open the Trino UI at http://localhost:8082 to run queries interactively.

### 6. Visualize in Superset

1. Open http://localhost:8088
2. Login with admin credentials (see .env)
3. Add Trino as a database connection: `trino://trino@trino:8080/nessie`
4. Create dashboards

## Available Make Targets

```bash
make help          # Show all available targets
make up            # Start all services
make down          # Stop all services
make status        # Show service status
make check         # Run health checks
make seed          # Seed MySQL (15 tables) + apply V2 enrichment
make produce       # Start JSON vitals producer (used by the demo)
make produce-avro  # Start Avro vitals producer (telemetry.vitals topic)
make docker-produce # Start Avro vitals producer against the compose stack
make migrate       # MySQL -> Iceberg (AVRO) in Nessie/MinIO + parity check
make validate      # Re-run the data parity check
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
├── main.py                      # CSV loader (FDNY -> Trino/Parquet)
├── produce_vitals.py            # Kafka vitals producer (Avro, local)
├── docker_produce_vitals.py     # Kafka vitals producer (Avro, Docker)
├── producer.py                  # Kafka vitals producer (JSON, used by demo)
├── generate_vitals.py           # Offline Avro file writer
├── avro_deserializer.py         # Kafka Avro consumer utility
├── schema_viewer.py             # Schema Registry viewer
├── check_stock.py               # Stack health checker
├── sql/                         # MySQL seed data (15 tables)
├── scripts/migration/           # Trino migration SQL
├── demo_script/                 # Demo queries
├── trino/catalog/               # Trino connector configs
├── trino_kafka/                 # Kafka table definitions
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
| `vitals` | JSON | Real-time patient vitals (used by the demo; joins MySQL by `patient_id` string) |
| `telemetry.vitals` | Avro | Experimental Avro vitals stream (integer patient IDs, Schema Registry) |

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
- Check Trino MySQL catalog: `trino/catalog/mysql.properties`

### Kafka topic not found

- Ensure Kafka is healthy: `docker compose ps kafka`
- Create the topic manually:
  ```bash
  docker exec kafka kafka-topics --bootstrap-server localhost:29092 \
    --create --topic telemetry.vitals --partitions 1 --replication-factor 1
  ```
- The JSON `vitals` topic is created automatically by the producer on first send.

### Federated join returns no rows

- Causes: the `V2__enrich_patients_for_kafka` migration hasn't run (so MySQL
  `patient_id` is NULL), or the producer hasn't sent any records yet.
- Fixes:
  ```bash
  make seed    # re-applies the V2 enrichment (idempotent)
  uv run producer.py   # then re-run the demo query
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

## Security Notes

- All credentials in this demo are for **local development only**
- Never commit real passwords to version control
- Use environment variables (`.env`) for all secrets
- Bind ports to `127.0.0.1` in production

## License

This project is for demonstration and educational purposes.
