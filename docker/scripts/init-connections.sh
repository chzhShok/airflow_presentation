#!/bin/bash

set -e

echo "=== Creating Airflow connections ==="

if ! airflow connections get postgres_default 2>/dev/null; then
    echo "Creating postgres_default connection..."
    airflow connections add postgres_default \
        --conn-type postgres \
        --conn-host postgres \
        --conn-login airflow \
        --conn-password airflow \
        --conn-port 5432 \
        --conn-schema airflow
    echo "Connection postgres_default created"
else
    echo "Connection postgres_default already exists"
fi

echo "=== Connections initialization completed ==="
