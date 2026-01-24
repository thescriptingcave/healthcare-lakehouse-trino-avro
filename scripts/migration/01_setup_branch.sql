-- Create the healthcare schema in MinIO if it doesn't exist
CREATE SCHEMA IF NOT EXISTS minio.healthcare;

-- Create the development branch for Avro migration
-- Note: This is usually run in the Nessie CLI or Spark, 
-- but in Trino we set the session to point to it.
SET SESSION minio.reference_name = 'dev_avro';