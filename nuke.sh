#!/bin/bash

echo "[INFO] Starting project teardown."

echo "[INFO] Stopping and removing Docker containers, volumes, and orphaned containers."
docker compose down --volumes --remove-orphans || { echo "[ERROR] Failed to stop and remove Docker containers." ; exit 1; }

echo "[INFO] Removing the virtual environment."
rm -rf venv || { echo "[ERROR] Failed to remove virtual environment." ; exit 1; }