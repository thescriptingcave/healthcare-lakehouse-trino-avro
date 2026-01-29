#!/usr/bin/env python3
"""
Simple Kafka producer for vitals data using JSON format
No Schema Registry required
"""

from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'vitals'

# Create producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_vitals():
    """Generate random vital signs data"""
    return {
        'patient_id': f'P{random.randint(1000, 9999)}',
        'heart_rate': random.randint(60, 100),
        'blood_pressure_systolic': random.randint(110, 140),
        'blood_pressure_diastolic': random.randint(70, 90),
        'temperature': round(random.uniform(36.5, 37.5), 1),
        'timestamp': int(datetime.now().timestamp() * 1000)
    }

def main():
    print(f"Starting producer for topic: {TOPIC_NAME}")
    print("Press Ctrl+C to stop\n")
    
    try:
        message_count = 0
        while True:
            vitals_data = generate_vitals()
            
            # Send to Kafka
            future = producer.send(TOPIC_NAME, value=vitals_data)
            
            # Wait for send confirmation
            record_metadata = future.get(timeout=10)
            
            message_count += 1
            print(f"[{message_count}] Sent: {vitals_data}")
            print(f"    → Partition: {record_metadata.partition}, Offset: {record_metadata.offset}\n")
            
            time.sleep(2)  # Send every 2 seconds
            
    except KeyboardInterrupt:
        print(f"\n\nStopping producer. Total messages sent: {message_count}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()