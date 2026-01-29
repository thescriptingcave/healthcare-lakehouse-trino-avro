import time
import random
from datetime import datetime
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# --- NEW FUNCTION START ---
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        # This confirms it successfully hit the broker AND passed schema validation
        print(f"✅ Registered & Delivered: {msg.topic()} [Partition: {msg.partition()}]")
# --- NEW FUNCTION END ---

# 1. Define the Avro Schema
value_schema_str = """
{
  "namespace": "healthcare.analytics",
  "type": "record",
  "name": "Vitals",
  "fields": [
    {"name": "patient_id", "type": "int"},
    {"name": "heart_rate", "type": "int"},
    {"name": "timestamp", "type": "long"}
  ]
}
"""

# 2. Configuration
sr_config = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(sr_config)
avro_serializer = AvroSerializer(schema_registry_client, value_schema_str)

producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'value.serializer': avro_serializer
}

producer = SerializingProducer(producer_config)

print("💓 Medical Device Simulator Started...")

try:
    while True:
        vitals_data = {
            "patient_id": random.randint(1, 5),
            "heart_rate": random.randint(65, 120),
            "timestamp": int(time.time())
        }

        # --- UPDATED PRODUCE CALL ---
        producer.produce(
            topic='telemetry.vitals', 
            value=vitals_data,
            on_delivery=delivery_report  # <--- Hooking up the reporter
        )
        
        # Flush is important to force the connection to happen immediately
        producer.poll(0) 
        time.sleep(1)
        producer.flush()
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping simulation...")