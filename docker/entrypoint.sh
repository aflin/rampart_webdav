#!/bin/sh
set -e

# Ensure data dir is owned by rampart user
chown -R rampart:rampart /app/data

# On first run, create admin user from env vars
if [ -n "$ADMIN_USER" ] && [ -n "$ADMIN_PASS" ]; then
    su -s /bin/sh rampart -c "rampart apps/webdav/webdav.js add '$ADMIN_USER' '$ADMIN_PASS'" 2>/dev/null && \
        su -s /bin/sh rampart -c "rampart apps/webdav/webdav.js admin '$ADMIN_USER' true" 2>/dev/null && \
        echo "Created admin user: $ADMIN_USER" || true
fi

# Wait for ONLYOFFICE to be healthy before starting the server
if [ -n "$OO_JWT_SECRET" ]; then
    echo "Waiting for ONLYOFFICE to become healthy..."
    TRIES=0
    MAX_TRIES=300
    HEALTH=""
    while [ $TRIES -lt $MAX_TRIES ]; do
        HEALTH=$(wget -qO- http://onlyoffice:80/healthcheck 2>/dev/null || true)
        if [ "$HEALTH" = "true" ]; then
            echo "  Healthy!"
            break
        fi
        TRIES=$((TRIES + 1))
        if [ $((TRIES % 10)) -eq 0 ]; then
            echo "  Still waiting... ($TRIES seconds)"
        fi
        sleep 1
    done

    if [ "$HEALTH" != "true" ]; then
        echo "WARNING: ONLYOFFICE not ready after $MAX_TRIES seconds. Starting without it."
    fi
fi

exec su -s /bin/sh rampart -c "rampart web_server_conf.js start"
