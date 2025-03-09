#!/bin/bash

set -e

echo "[INFO] Starting project setup."

echo "[INFO] Creating a virtual environment."
python3 -m venv venv || { echo "[ERROR] Failed to create virtual environment." ; exit 1; }

echo "[INFO] Activating the virtual environment."
source ./venv/bin/activate || { echo "[ERROR] Failed to activate virtual environment." ; exit 1; }

echo "[INFO] Upgrading pip, wheel, and setuptools."
pip install --upgrade pip wheel setuptools || { echo "[ERROR] Failed to upgrade pip, wheel, and setuptools." ; exit 1; }

echo "[INFO] Installing dependencies from requirements.txt."
if [ ! -f requirements.txt ]; then
    echo "[ERROR] requirements.txt not found."
    exit 1
fi
pip install --no-cache-dir -r requirements.txt || { echo "[ERROR] Failed to install dependencies." ; exit 1; }

echo "[INFO] Starting Docker containers."
docker compose up -d || { echo "[ERROR] Failed to start Docker containers." ; exit 1; }

echo "[INFO] Preparing the database."
if [ ! -f ./_prepare_database.py ]; then
    echo "[ERROR] _prepare_database.py not found."
    exit 1
fi
python ./_prepare_database.py || { echo "[ERROR] Database preparation failed." ; exit 1; }

echo "[INFO] Ingesting data into DataHub."
if [ ! -f ./recipes/inventory.yml ]; then
    echo "[ERROR] inventory.yml not found."
    exit 1
fi
datahub ingest -c ./recipes/inventory.yml || { echo "[ERROR] Data ingestion failed." ; exit 1; }

echo "[INFO] Running the main application."
if [ ! -f ./app/app.py ]; then
    echo "[ERROR] app.py not found."
    exit 1
fi
python ./app/app.py || { echo "[ERROR] Application execution failed." ; exit 1; }

echo "[INFO] Project setup completed successfully."
exit 0