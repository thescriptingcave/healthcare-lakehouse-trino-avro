-- Create the healthcare schema in the Nessie catalog if it doesn't exist.
-- The migration CTAS scripts (02_migration_core.sql) write into this schema
-- on the default branch ("main") configured in trino/catalog/nessie.properties.
CREATE SCHEMA IF NOT EXISTS nessie.healthcare;