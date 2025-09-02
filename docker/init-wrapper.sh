#!/bin/bash

set -e

echo "=== Starting Airflow initialization ==="

/opt/airflow/init-connections.sh

/opt/airflow/init-pools.sh

echo "=== All initialization completed ==="
