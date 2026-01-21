import json
import time
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationError

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC_NAME = 'healthcare_server.healthcare.patients'

def main():
    sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(sr_client)

    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'avro-tester-group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([TOPIC_NAME])

    print(f"📡 Monitoring {TOPIC_NAME}...")

    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                # Corrected error code check
                if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    print(f"⚠️ Topic not found yet. Still waiting for Debezium to sync table...")
                    time.sleep(5)
                else:
                    print(f"❌ Kafka Error: {msg.error()}")
                continue

            try:
                decoded_data = avro_deserializer(msg.value(), None)
                if decoded_data and 'after' in decoded_data:
                    p = decoded_data['after']
                    print(f"\n✅ SUCCESS! Patient Found: {p.get('FIRST')} {p.get('LAST')}")
                    break 

            except SerializationError:
                continue # Skip non-Avro noise
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        consumer.close()

if __name__ == '__main__':
    main()