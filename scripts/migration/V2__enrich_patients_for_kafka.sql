-- Migration: V2__enrich_patients_for_kafka.sql
-- Purpose: Add and populate short IDs to bridge MySQL patients with the Kafka vitals stream.
-- Requires: Run via `mysql healthcare < scripts/migration/V2__enrich_patients_for_kafka.sql`

-- 1. Add the patient_id column if it doesn't exist.
-- MySQL 8.0 has no ADD COLUMN IF NOT EXISTS, so we use a dynamic check
-- against information_schema and prepare the ALTER statement only when needed.
SET @col_exists := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'patients'
      AND column_name = 'patient_id'
);

SET @sql := IF(@col_exists = 0,
    'ALTER TABLE patients ADD COLUMN patient_id VARCHAR(10) AFTER id',
    'SELECT "patient_id column already exists" AS status');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 2. Populate the IDs for the 8 patients referenced by the vitals stream.
-- These mappings must match rows that actually exist in the Synthea data set
-- (verified against scripts/sql/HEALTHCARE_patients.sql). First+last name pairs
-- are unique because Synthea appends an index suffix (e.g. "Leilani136").
UPDATE healthcare.patients SET patient_id = 'P5469' WHERE first = 'Leilani136' AND last = 'Romaguera67';
UPDATE healthcare.patients SET patient_id = 'P8250' WHERE first = 'Kellye282' AND last = 'Schuppe920';
UPDATE healthcare.patients SET patient_id = 'P3194' WHERE first = 'Clint766' AND last = 'Deckow585';
UPDATE healthcare.patients SET patient_id = 'P1001' WHERE first = 'Ty725' AND last = 'Beatty507';
UPDATE healthcare.patients SET patient_id = 'P2002' WHERE first = 'Reyna401' AND last = 'Shanahan202';
UPDATE healthcare.patients SET patient_id = 'P3003' WHERE first = 'Don899' AND last = 'Upton904';
UPDATE healthcare.patients SET patient_id = 'P4004' WHERE first = 'Tabetha269' AND last = 'Predovic534';
UPDATE healthcare.patients SET patient_id = 'P5005' WHERE first = 'Kay203' AND last = 'Muller251';

-- 3. Add an index to speed up the Federated Join in Trino.
SET @idx_exists := (
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name = 'patients'
      AND index_name = 'idx_patient_id'
);

SET @idx_sql := IF(@idx_exists = 0,
    'CREATE INDEX idx_patient_id ON healthcare.patients(patient_id)',
    'SELECT "patient_id index already exists" AS status');
PREPARE idx_stmt FROM @idx_sql;
EXECUTE idx_stmt;
DEALLOCATE PREPARE idx_stmt;