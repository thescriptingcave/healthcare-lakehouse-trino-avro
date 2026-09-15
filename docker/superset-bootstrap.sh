#!/usr/bin/env bash
#
# Superset bootstrap: installs the Trino driver, provisions the admin user,
# upgrades the metadata DB, and initializes roles/permissions before starting
# the web server. Idempotent - safe to re-run on every container start.
set -euo pipefail

pip install -r /app/requirements-local.txt

# Idempotent: create-admin exits non-zero if the user already exists.
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
  --firstname "${SUPERSET_ADMIN_FIRSTNAME:-Admin}" \
  --lastname "${SUPERSET_ADMIN_LASTNAME:-User}" \
  --email "${SUPERSET_ADMIN_EMAIL:-admin@superset.local}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true

superset db upgrade
superset init

exec /usr/bin/run-server.sh "$@"