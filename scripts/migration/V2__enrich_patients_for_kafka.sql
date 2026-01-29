-- Migration: V2__enrich_patients_for_kafka.sql
-- Purpose: Add and populate short IDs to bridge MySQL patients with Avro vitals stream.

-- 1. Add the patient_id column if it doesn't exist
-- This ensures we don't break the existing UUID-based primary key.
ALTER TABLE healthcare.patients 
ADD COLUMN IF NOT EXISTS patient_id VARCHAR(10) AFTER id;

-- 2. Populate the IDs for the 8 patients currently in the Avro stream
-- These mappings are required for the Trino federated join to succeed.
UPDATE healthcare.patients SET patient_id = 'P5469' WHERE first = 'Jose' AND last = 'Eduardo';
UPDATE healthcare.patients SET patient_id = 'P8250' WHERE first = 'Milo' AND last = 'Winter';
UPDATE healthcare.patients SET patient_id = 'P3194' WHERE first = 'Jayson' AND last = 'Lynch';
-- Add the remaining 5 mappings here...

-- 3. Add an index to speed up the Federated Join in Trino
CREATE INDEX idx_patient_id ON healthcare.patients(patient_id);