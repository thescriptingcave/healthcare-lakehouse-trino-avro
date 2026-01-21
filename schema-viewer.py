from confluent_kafka.schema_registry import SchemaRegistryClient
import json

sr_client = SchemaRegistryClient({'url': 'http://localhost:8081'})

# Request the latest "contract" for patients
subject = "healthcare_server.healthcare.patients-value"
schema_obj = sr_client.get_latest_version(subject)

# The actual Avro schema is stored as a string inside the object
schema_dict = json.loads(schema_obj.schema.schema_str)

print(f"--- Schema for {subject} ---")
print(json.dumps(schema_dict, indent=2))