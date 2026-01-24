-- 1. Load the required plugins
LOAD mysql;
LOAD httpfs;

-- 2. Create the "Bridge" to your 15 Medical Tables
-- Using your confirmed 'rootpassword' and 'HEALTHCARE' DB
ATTACH 'host=127.0.0.1 user=root password=rootpassword port=3306 db=HEALTHCARE' AS my_sql_db (TYPE MYSQL);

-- 3. Create the "Key" to your MinIO Bucket
-- Using your confirmed 'minio' / 'minio123' credentials
CREATE OR REPLACE SECRET minio_secret (
    TYPE S3,
    PROVIDER CONFIG,
    KEY_ID 'minio',
    SECRET 'minio123',
    ENDPOINT '127.0.0.1:9000',
    URL_STYLE 'path',
    USE_SSL false,
    REGION 'us-east-1'
);

-- 4. The "Spoof & Move"
-- This pulls from MySQL, fixes the Zip, and lands it in MinIO
COPY (
    SELECT *, '11201' AS zip 
    FROM my_sql_db.patients 
    LIMIT 1000
) TO 's3://healthcare/patients_spoofed.parquet' (FORMAT 'PARQUET');