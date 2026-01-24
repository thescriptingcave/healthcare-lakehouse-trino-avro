-- Check Row Counts (Should be 80,751 for Observations)
SELECT 'Avro Count' as source, COUNT(*) as total FROM minio.healthcare.observations
UNION ALL
SELECT 'MySQL Count' as source, COUNT(*) as total FROM mysql.healthcare.observations;

-- Check Unique Patients
SELECT 'Avro Patients' as source, COUNT(DISTINCT patient_id) FROM minio.healthcare.observations
UNION ALL
SELECT 'MySQL Patients' as source, COUNT(DISTINCT PATIENT) FROM mysql.healthcare.observations;

-- Clinical Logic Join Test
SELECT 
    p.first, 
    p.last, 
    AVG(o.observation_value) as avg_bmi
FROM minio.healthcare.patients p
JOIN minio.healthcare.observations o ON p.id = o.patient_id
WHERE o.description = 'Body Mass Index'
GROUP BY p.first, p.last
ORDER BY avg_bmi DESC;