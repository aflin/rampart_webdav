#!/bin/bash
#
# ONLYOFFICE Document Server Setup Script
#
# This script sets up ONLYOFFICE from scratch:
#   1. Pulls the Docker image
#   2. Starts the container and waits for it to become healthy
#
# Usage: ./setup.sh
#

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

COMPOSE="$SCRIPT_DIR/docker-compose.yml"

if [ ! -f "$COMPOSE" ]; then
    echo "Error: docker-compose.yml not found in $SCRIPT_DIR"
    exit 1
fi

# Detect docker compose command
if docker compose version > /dev/null 2>&1; then
    DOCKER_COMPOSE="docker compose"
elif command -v docker-compose > /dev/null 2>&1; then
    DOCKER_COMPOSE="docker-compose"
else
    echo "Error: Neither 'docker compose' nor 'docker-compose' found."
    exit 1
fi

# Check if docker is available
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running or not accessible."
    echo "Try running this script with sudo, or add your user to the docker group."
    exit 1
fi

# Check if already running
RUNNING=$(docker inspect -f '{{.State.Running}}' onlyoffice 2>/dev/null)
if [ "$RUNNING" = "true" ]; then
    echo "ONLYOFFICE container is already running."
    read -p "Start fresh? This will remove existing data. [y/N] " FRESH
    if [ "$FRESH" != "y" ] && [ "$FRESH" != "Y" ]; then
        echo "Aborted."
        exit 0
    fi
fi

# Clean up
docker rm -f onlyoffice 2>/dev/null || true
$DOCKER_COMPOSE -f "$COMPOSE" down 2>/dev/null || true

echo "==> Pulling ONLYOFFICE Docker image..."
docker pull onlyoffice/documentserver

echo "==> Starting ONLYOFFICE container..."
if ! $DOCKER_COMPOSE -f "$COMPOSE" up -d; then
    echo "Error: Failed to start container."
    exit 1
fi

echo "==> Waiting for ONLYOFFICE to become healthy..."
echo "    (This typically takes 2-5 minutes on first start)"
TRIES=0
MAX_TRIES=300
HEALTH=""
while [ $TRIES -lt $MAX_TRIES ]; do
    HEALTH=$(curl -s http://127.0.0.1:6659/healthcheck 2>/dev/null || true)
    if [ "$HEALTH" = "true" ]; then
        echo "    Healthy!"
        break
    fi
    TRIES=$((TRIES + 1))
    if [ $((TRIES % 10)) -eq 0 ]; then
        echo "    Still waiting... ($TRIES seconds)"
    fi
    sleep 1
done

if [ "$HEALTH" != "true" ]; then
    echo ""
    echo "Error: ONLYOFFICE did not become healthy after $MAX_TRIES seconds."
    echo "Check: docker logs onlyoffice"
    exit 1
fi

echo ""
echo "ONLYOFFICE is ready!"
echo ""
echo "Next steps:"
echo "  Restart the Rampart web server to detect ONLYOFFICE:"
echo "    rampart web_server_conf.js restart"
