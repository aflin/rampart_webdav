#!/bin/bash
#
# ONLYOFFICE Document Server Setup Script
#
# This script sets up ONLYOFFICE from scratch:
#   1. Starts the container to let the entrypoint generate config
#   2. Waits for the healthcheck to pass
#   3. Copies out the generated local.json
#   4. Stops the container
#   5. Optionally sets storage.externalHost (for reverse proxy setups)
#   6. Adds the volume mount and restarts
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

# Check if docker is available
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running or not accessible."
    echo "Try running this script with sudo, or add your user to the docker group."
    exit 1
fi

# Check if container is already running with a local.json mount
if [ -f "$SCRIPT_DIR/local.json" ]; then
    echo "local.json already exists in $SCRIPT_DIR"
    read -p "Start fresh? This will remove existing data and config. [y/N] " FRESH
    if [ "$FRESH" != "y" ] && [ "$FRESH" != "Y" ]; then
        echo "Aborted."
        exit 0
    fi
    docker compose -f "$COMPOSE" down 2>/dev/null || true
    rm -rf "$SCRIPT_DIR/data" "$SCRIPT_DIR/local.json"
fi

# Ensure docker-compose.yml does NOT have volumes mount yet
# (use a temp copy without volumes for first boot)
TMPCOMPOSE=$(mktemp)
grep -v 'local.json' "$COMPOSE" | grep -v '^\s*volumes:' | grep -v '^\s*- \./local' > "$TMPCOMPOSE"

echo "==> Pulling ONLYOFFICE Docker image..."
docker pull onlyoffice/documentserver

# Remove any existing container
docker rm -f onlyoffice 2>/dev/null || true

echo "==> Starting ONLYOFFICE container for initial setup..."
docker compose -f "$TMPCOMPOSE" up -d
rm -f "$TMPCOMPOSE"

echo "==> Waiting for ONLYOFFICE to become healthy..."
echo "    (This typically takes 2-5 minutes on first start)"
TRIES=0
MAX_TRIES=120
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
    echo "Error: ONLYOFFICE did not become healthy after $MAX_TRIES seconds."
    echo "Check: docker logs onlyoffice"
    exit 1
fi

echo "==> Extracting generated local.json from container..."
docker cp onlyoffice:/etc/onlyoffice/documentserver/local.json "$SCRIPT_DIR/local.json"

echo "==> Stopping container..."
docker rm -f onlyoffice 2>/dev/null || true
docker compose -f "$COMPOSE" down 2>/dev/null || true

# Ask about externalHost
echo ""
echo "If Rampart is behind a reverse proxy (e.g. nginx with TLS),"
echo "you need to set the external URL so ONLYOFFICE generates"
echo "correct cache file URLs."
echo ""
echo "If Rampart handles TLS directly, just press Enter to skip."
echo ""
read -p "External URL (e.g. https://example.com) or Enter to skip: " EXT_HOST

if [ -n "$EXT_HOST" ]; then
    python3 - "$SCRIPT_DIR/local.json" "$EXT_HOST" <<'PYEOF'
import json, sys
f = sys.argv[1]
host = sys.argv[2]
d = json.load(open(f))
d.setdefault('storage', {})['externalHost'] = host
json.dump(d, open(f, 'w'), indent=2)
print('Set externalHost to: ' + host)
PYEOF
fi

# Ensure docker-compose.yml has the volumes mount
if ! grep -q 'local.json' "$COMPOSE"; then
    # Insert volumes section after the ports section
    python3 -c "
import re
with open('$COMPOSE') as f:
    text = f.read()
if 'volumes:' not in text:
    text = re.sub(
        r'(    ports:\n      - \"6659:80\")\n',
        r'\1\n    volumes:\n      - ./local.json:/etc/onlyoffice/documentserver/local.json\n',
        text
    )
    with open('$COMPOSE', 'w') as f:
        f.write(text)
    print('Added volumes mount to docker-compose.yml')
else:
    print('docker-compose.yml already has volumes section')
"
fi

echo "==> Starting ONLYOFFICE with mounted local.json..."
docker compose -f "$COMPOSE" up -d

echo "==> Waiting for ONLYOFFICE to become healthy..."
TRIES=0
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
    echo "Warning: ONLYOFFICE not yet healthy after $MAX_TRIES seconds."
    echo "It may still be starting. Check: docker logs onlyoffice"
    echo ""
    echo "Setup complete. Restart the Rampart web server to detect ONLYOFFICE."
    exit 0
fi

# Apply externalHost patch inside the running container
# (must be done after startup because the entrypoint overwrites local.json)
if [ -n "$EXT_HOST" ]; then
    echo "==> Patching externalHost inside running container..."

    # Read the secure link secret from nginx inside the container
    SECLINK=$(docker exec onlyoffice grep -oP 'secure_link_secret \K[^;]+' /etc/nginx/includes/ds-docservice.conf 2>/dev/null)
    if [ -z "$SECLINK" ]; then
        SECLINK="verysecretstring"
    fi

    # Read JWT secret from docker-compose.yml
    SECRET=$(grep 'JWT_SECRET=' "$COMPOSE" | sed 's/.*JWT_SECRET=//' | tr -d ' ')

    docker exec onlyoffice python3 - "$EXT_HOST" "$SECRET" "$SECLINK" <<'PYEOF'
import json, sys
host = sys.argv[1]
secret = sys.argv[2]
seclink = sys.argv[3]
f = '/etc/onlyoffice/documentserver/local.json'
try:
    d = json.load(open(f))
except:
    d = {}
d.setdefault('storage', {})['externalHost'] = host
d['storage'].setdefault('fs', {})['secretString'] = seclink
co = d.setdefault('services', {}).setdefault('CoAuthoring', {})
sec = co.setdefault('secret', {})
for k in ['browser', 'inbox', 'outbox', 'session']:
    sec[k] = {'string': secret}
tok = co.setdefault('token', {}).setdefault('enable', {})
tok['request'] = {'inbox': True, 'outbox': True}
tok['browser'] = True
co['requestDefaults'] = {'rejectUnauthorized': False}
json.dump(d, open(f, 'w'), indent=2)
print('    Patched: externalHost=' + host)
PYEOF
    docker exec onlyoffice supervisorctl restart ds:docservice
    echo "    Docservice restarted."
    sleep 3
fi

echo ""
echo "ONLYOFFICE is ready!"
echo "Setup complete. Restart the Rampart web server to detect ONLYOFFICE."
