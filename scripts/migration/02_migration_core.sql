-- 1. Patients
CREATE TABLE minio.healthcare.patients 
WITH (format = 'AVRO') 
AS SELECT * FROM mysql.healthcare.patients;

-- 2. Encounters
CREATE TABLE minio.healthcare.encounters 
WITH (format = 'AVRO') 
AS SELECT * FROM mysql.healthcare.encounters;

-- 3. Allergies (Handling Reserved Keywords and Nulls)
CREATE TABLE minio.healthcare.allergies 
WITH (format = 'AVRO') 
AS SELECT 
    "START" as start_date, 
    CASE WHEN "STOP" = '' THEN NULL ELSE "STOP" END as stop_date,
    "PATIENT" as patient_id,
    "ENCOUNTER" as encounter_id,
    "CODE",
    "DESCRIPTION"
FROM mysql.healthcare.allergies;

-- 4. Observations (The 80k row table)
CREATE TABLE minio.healthcare.observations 
WITH (format = 'AVRO') 
AS SELECT 
    from_iso8601_timestamp("date") as observation_time, 
    "patient" as patient_id, 
    "encounter" as encounter_id, 
    "code" as observation_code, 
    "description", 
    "value" as observation_value, 
    "units", 
    "type"
FROM mysql.healthcare.observations;