#!/bin/bash
set -euo pipefail

# Install requirements if present
if [ -f "/opt/airflow/requirements.txt" ]; then
    pip install --upgrade pip
    pip install --no-cache-dir -r /opt/airflow/requirements.txt
fi

# Run the command passed to the container (scheduler, api-server, dag-processor, etc.)
exec airflow "$@"