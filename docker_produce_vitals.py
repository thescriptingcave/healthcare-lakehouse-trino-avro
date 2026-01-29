import time
import random
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

# --- CONFIGURATION (DOCKER INTERNAL) ---
conf = {
    'bootstrap.servers': 'kafka:29092', # Using internal Docker port
    'client.id': 'docker-vital-producer'
}

schema_registry_conf = {'url': 'http://schema-registry:8081'} # Using internal service name
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# --- AVRO SCHEMA ---
# Match the original schema exactly to satisfy the Registry's compatibility check
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

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Registered & Delivered: {msg.topic()} Partition {msg.partition()}")

# Initialize Serializers
avro_serializer = AvroSerializer(schema_registry_client, value_schema_str)
string_serializer = StringSerializer('utf_8')
producer = Producer(conf)

print("🚀 Docker Producer started. Sending live vitals...")

try:
    while True:
        # Simulate patient data
        patient_id = random.randint(1, 10)
        heart_rate = random.randint(60, 100)
        timestamp = int(time.time())

        data = {
            "patient_id": patient_id, 
            "heart_rate": heart_rate,
            "timestamp": int(time.time()) # Add this line
        }

        producer.produce(
            topic='telemetry.vitals',
            key=string_serializer(str(patient_id)),
            value=avro_serializer(data, SerializationContext('telemetry.vitals', MessageField.VALUE)),
            on_delivery=delivery_report
        )
        producer.flush()
        time.sleep(2)
except KeyboardInterrupt:
    print("🛑 Producer stopped.")