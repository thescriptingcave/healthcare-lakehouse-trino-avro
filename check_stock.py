import requests
import json

def check_health():
    print("🏥 Starting Healthcare Stack Health Check...\n")
    
    # 1. Check Schema Registry (Crucial for Avro)
    try:
        sr_response = requests.get("http://localhost:8081/subjects")
        if sr_response.status_code == 200:
            print("✅ [Schema Registry]: Online and ready for Avro schemas.")
        else:
            print("⚠️ [Schema Registry]: Online but returned status:", sr_response.status_code)
    except Exception as e:
        print("❌ [Schema Registry]: Unreachable. Ensure container 'schema-registry' is running.")

    # 2. Check Nessie (The Metadata Map)
    try:
        nessie_response = requests.get("http://localhost:19120/api/v1/config")
        if nessie_response.status_code == 200:
            print("✅ [Nessie]: Catalog REST API is responding.")
    except Exception as e:
        print("❌ [Nessie]: Unreachable. Check the 'nessie' container.")

    # 3. Check Kafka UI (Redpanda Console)
    try:
        ui_response = requests.get("http://localhost:8080/health")
        if ui_response.status_code == 200:
            print("✅ [Kafka UI]: Redpanda Console is active at http://localhost:8080")
    except Exception as e:
        # Redpanda console might not have a /health endpoint, checking status code 200 on root
        print("ℹ️  [Kafka UI]: Check http://localhost:8080 manually to view topics.")

    print("\n🚀 If all checks passed, we are ready to stream Avro data.")

if __name__ == "__main__":
    check_health()