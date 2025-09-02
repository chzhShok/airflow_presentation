#!/bin/bash

set -e

echo "=== Creating Airflow pool ==="

if ! airflow pools get ml_pool 2>/dev/null; then
    echo "Creating ml_pool..."
    airflow pools set ml_pool 10 "test pool"
    echo "Pool ml_pool created with 10 slots"
else
    echo "Pool ml_pool already exists"
fi

echo "=== Pool initialization completed ==="
