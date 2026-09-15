#!/usr/bin/env python3
"""
Stack health check utility.

Verifies connectivity to all services in the healthcare lakehouse stack.
"""

import logging
import os
import sys

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration from environment or defaults
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
NESSIE_URL = os.getenv("NESSIE_URL", "http://localhost:19120")
KAFKA_UI_URL = os.getenv("KAFKA_UI_URL", "http://localhost:8080")
TRINO_URL = os.getenv("TRINO_URL", "http://localhost:8082")
SUPerset_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")


def check_service(name: str, url: str, timeout: int = 5) -> bool:
    """Check if a service is healthy.

    Args:
        name: Display name of the service.
        url: Health check URL.
        timeout: Request timeout in seconds.

    Returns:
        True if the service is healthy, False otherwise.
    """
    try:
        response = requests.get(url, timeout=timeout)
        if response.status_code == 200:
            logger.info("[OK]      %s is online", name)
            return True
        logger.warning("[WARN]    %s returned status %d", name, response.status_code)
        return False
    except requests.RequestException:
        logger.error("[FAIL]    %s is unreachable at %s", name, url)
        return False


def run_health_checks() -> bool:
    """Run health checks on all stack services.

    Returns:
        True if all critical services are healthy.
    """
    logger.info("Starting Healthcare Stack Health Check...")
    logger.info("")

    results = [
        check_service("Schema Registry", f"{SCHEMA_REGISTRY_URL}/subjects"),
        check_service("Nessie", f"{NESSIE_URL}/api/v1/config"),
        check_service("Kafka UI", KAFKA_UI_URL),
        check_service("Trino", f"{TRINO_URL}/v1/info"),
        check_service("Superset", f"{SUPerset_URL}/health"),
    ]

    logger.info("")
    if all(results):
        logger.info("All services are healthy. Ready to stream data.")
        return True
    logger.warning("Some services are not healthy. Check the logs above.")
    return False


def main() -> None:
    """Main entry point."""
    healthy = run_health_checks()
    sys.exit(0 if healthy else 1)


if __name__ == "__main__":
    main()
