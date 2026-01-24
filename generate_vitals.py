import os
import time
from fastavro import writer, parse_schema

# 1. Define the Schema (Must match your Trino 'CREATE TABLE' exactly)
# 1. Update the 'type' for patient_id to 'string'
schema = {
    'doc': 'Patient Vital Signs',
    'name': 'vitals',
    'type': 'record',
    'fields': [
        {'name': 'patient_id', 'type': 'string'},  # CHANGED FROM 'int' TO 'string'
        {'name': 'heart_rate', 'type': 'int'},
        {'name': 'blood_pressure_sys', 'type': 'int'},
        {'name': 'ts', 'type': 'long'},
    ],
}
parsed_schema = parse_schema(schema)

# 2. Generate some "Fake" Medical Data
# Use IDs that you know are in your MySQL patients table
records = [
    {
        "patient_id": "110339b10-3cd1-4ac3-ac13-ec26728cb592", 
        "heart_rate": 78, 
        "blood_pressure_sys": 125, 
        "ts": int(time.time())
    }
]

# 3. Path to your MinIO data folder
# CHANGE THIS to your actual local path
# Change the underscore to a dash
output_path = './minio-data/healthcare-avro/vitals/patient_data.avro'

# 4. Write the file
with open(output_path, 'wb') as out:
    writer(out, parsed_schema, records)

print(f"Success! Avro file created at {output_path}")