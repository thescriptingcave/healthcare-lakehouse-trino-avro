# SQL Tutorials: Time-Series, CTEs, and Window Functions

Hands-on SQL lessons built on business questions an analyst would actually be
asked, run against **this** lakehouse stack. Every query in these files executes
against the live data, so open results next to the explanations.

## The data you will work with

```
 Catalog   Schema             Tables you'll use
 --------- -----------------  --------------------------------------------------
 kafka     default            "telemetry.vitals"   live vitals stream (Avro)
 mysql     healthcare         patients             enriched with patient_id (P####)
 nessie    healthcare         patients  encounters  observations       (Synthea)
 nessie    clinical_archive   high_vitals_history  archived high heart rates
```

Key columns and the join keys that tie them together:

| Table                          | Time column                              | Join key                        |
| ------------------------------ | ---------------------------------------- | ------------------------------- |
| `nessie.healthcare.observations` | `observation_time` (timestamp, clean)     | `patient_id` (UUID)             |
| `nessie.healthcare.encounters`   | `start` / `stop` (ISO-8601 **strings**)   | `patient` (UUID)                |
| `kafka.default."telemetry.vitals"` | `"timestamp"` (epoch **milliseconds**, INT) | `patient_id` (P####)        |
| `nessie.clinical_archive.high_vitals_history` | `"timestamp"` (epoch ms) | `patient_id` (P####) |
| `nessie.healthcare.patients`     | (none)                                   | `id` (UUID)                     |
| `mysql.healthcare.patients`      | (none)                                   | `id` (UUID), `patient_id` (P####) |

Two join bridges because Synthea tables use UUIDs while the Kafka stream and
archive use short `P####` ids:

* UUID world: `observations.patient_id` = `patients.id` = `encounters.patient`
* Stream world: `telemetry."patient_id"` = `patients.patient_id` (only 8
  patients are enriched, via `scripts/migration/V2__enrich_patients_for_kafka.sql`)
* Bridge: `mysql.healthcare.patients.id` (UUID) <-> `.patient_id` (P####)

## How to run a tutorial

Prerequisites: the stack is up and `make seed` + `make migrate` have run.

Run any tutorial end to end - each file is a sequence of labeled queries:

```bash
make tutorial TUT=01        # pipe the file through trino
# equivalent to:  docker exec -i trino trino < tutorials/01_time_series_basics.sql
```

One rule: expect some sections to return **no rows**. Empty results (e.g. a
HAVING that filters everything out) are a feature in these exercises, and the
comments call them out so you recognize them.

## Learning path

| #  | File                                    | Skills                                   | Business question you answer |
| -- | --------------------------------------- | ---------------------------------------- | ---------------------------- |
| 1  | `01_time_series_basics.sql`             | WHERE, aggregates, GROUP BY, date_trunc, CTEs, epoch-ms conversion | "Summarize vitals per encounter; which patients have the widest heart-rate range?" |
| 2  | `02_window_functions.sql`               | ROW_NUMBER, LAG/LEAD, moving averages, partition ordering, tie-breaking | "What does a patient's vitals trajectory look like, and who had the biggest jump?" |
| 3  | `03_running_totals_and_bucketing.sql`   | ISO-8601 parsing, monthly buckets, cumulative sums, YoY LAG, rolling 12-month average | "Is the business burning through encounter claim costs - are vitals spiking?" |
| 4  | `04_federated_realtime_analytics.sql`   | cross-catalog joins (Kafka+MySQL+Iceberg), CTE pipelines, anti-joins, idempotent INSERT | "Is anyone's live heart rate off their historical baseline right now?" |

## Where each tool shows up

* **Trino** - every query runs through Trino's SQL engine, federating all
  catalogs in one query.
* **MySQL** - the `patients` side of the stream joins (`mysql.healthcare.*`).
* **Kafka + Schema Registry** - the `telemetry.vitals` topic you query live.
* **Nessie + Iceberg + MinIO** - the `nessie.healthcare.*` time-series tables
  and the archival write in tutorial 4.
* **Superset** - tutorial 4 ends by visualizing the same data on the dashboard
  (and building your own line chart in Explore).
* **Makefile** - `make tutorial TUT=NN` to run each file.

## Tips

* `make tutorial` pipes the whole file at once, but you can also run a single
  section by highlighting it in any SQL editor pointed at
  `jdbc:trino://localhost:8082` (catalog `nessie`, schema `healthcare`).
* The telemetry topic only holds ~40 readings and is updated by
  `make produce`/`make docker-produce` - produce more data to see the alerts in
  tutorial 4 change.
* `from_unixtime(col / 1000)` converts the stream's epoch-milliseconds to a
  human timestamp; `from_iso8601_timestamp(col)` parses encounter start times.