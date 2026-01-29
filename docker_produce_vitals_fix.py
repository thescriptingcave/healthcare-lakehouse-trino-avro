import time
import random
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# --- 1. REGISTRY CLIENT SETUP ---
# This client handles the "Contract" with the Registry
sr_conf = {'url': 'http://schema-registry:8081'}
schema_registry_client = SchemaRegistryClient(sr_conf)

# --- 2. THE SCHEMA (Must match what we registered earlier) ---
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

# --- 3. SERIALIZER SETUP ---
# This converts Python Dicts -> Avro Binary
avro_serializer = AvroSerializer(schema_registry_client, value_schema_str)
string_serializer = StringSerializer('utf_8')

# --- 4. PRODUCER CONFIGURATION ---
# We bake the serializers directly into the producer here
producer_conf = {
    'bootstrap.servers': 'kafka:29092',
    'key.serializer': string_serializer,
    'value.serializer': avro_serializer,
    'client.id': 'docker-vital-producer'
}

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Registered & Delivered to: {msg.topic()} [{msg.partition()}]")

# Initialize the modern SerializingProducer
producer = SerializingProducer(producer_conf)

print("🚀 Serializing Producer started. Broadcasting to telemetry.vitals...")

try:
    while True:
        # Generate medical telemetry
        patient_id = random.randint(1, 10)
        heart_rate = random.randint(60, 110)
        
        # Prepare the record
        # Note: We use a simple dict. The producer handles the Avro conversion!
        data = {
            "patient_id": patient_id, 
            "heart_rate": heart_rate,
            "timestamp": int(time.time())
        }
        
        producer.produce(
            topic='telemetry.vitals',
            key=str(patient_id),
            value=data,
            on_delivery=delivery_report
        )
        
        # In production, you wouldn't flush every message, 
        # but it's great for seeing live results in the UI.
        producer.flush() 
        time.sleep(2)
        
except KeyboardInterrupt:
    print("🛑 Producer stopping...")