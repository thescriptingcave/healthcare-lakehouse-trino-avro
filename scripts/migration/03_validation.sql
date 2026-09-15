-- Check Row Counts
-- Should match between MySQL source and Iceberg lakehouse
SELECT 'Iceberg Count' as source, COUNT(*) as total FROM nessie.healthcare.observations
UNION ALL
SELECT 'MySQL Count' as source, COUNT(*) as total FROM mysql.healthcare.observations;

-- Check Unique Patients
SELECT 'Iceberg Patients' as source, COUNT(DISTINCT patient_id) FROM nessie.healthcare.observations
UNION ALL
SELECT 'MySQL Patients' as source, COUNT(DISTINCT PATIENT) FROM mysql.healthcare.observations;

-- Clinical Logic Join Test
-- Validates patient-observation linkage via federated join
-- Note: Iceberg normalizes column names to lowercase (e.g. id, first, last)
SELECT
    p.id AS patient_id,
    p.first AS first_name,
    p.last AS last_name,
    AVG(CAST(o.observation_value AS DOUBLE)) as avg_bmi
FROM nessie.healthcare.patients p
JOIN nessie.healthcare.observations o ON p.id = o.patient_id
WHERE o.description = 'Body Mass Index'
GROUP BY p.id, p.first, p.last
ORDER BY avg_bmi DESC;