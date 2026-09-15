-- Demonstrate joining live Kafka data with MySQL patients
-- This shows that you are merging a static database with a live message stream in real-time.
-- Live vitals arrive on the `telemetry.vitals` Kafka topic (Avro, Schema Registry).
-- The Trino Kafka connector auto-discovers the table from the Schema Registry
-- subject (kafka.table-description-supplier=CONFLUENT), so no file-based table
-- definition is needed.
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

-- Part 2: The "Iceberg Archive" Demo
-- This demonstrates how you can take that live data and "freeze" it into your Nessie catalog for long-term storage.

-- 1. Create a schema in Nessie if it doesn't exist
CREATE SCHEMA IF NOT EXISTS nessie.clinical_archive;

-- 2. Create a permanent Iceberg table from the live join (high vitals only)
CREATE TABLE IF NOT EXISTS nessie.clinical_archive.high_vitals_history AS
SELECT
    p.patient_id,
    p.first,
    v.heart_rate,
    v.timestamp
FROM kafka.default."telemetry.vitals" v
JOIN mysql.healthcare.patients p ON v.patient_id = p.patient_id
WHERE v.heart_rate > 100;

-- 3. Verify the data is now in Iceberg/MinIO
SELECT * FROM nessie.clinical_archive.high_vitals_history LIMIT 5;