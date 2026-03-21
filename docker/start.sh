#!/bin/sh
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

IMAGE="rampart-filemanager"

# Build or pull the image
if [ -f "$SCRIPT_DIR/build.sh" ]; then
    # Local development — build from source
    echo "Found build.sh — building image locally..."
    "$SCRIPT_DIR/build.sh"
elif ! docker image inspect "$IMAGE" > /dev/null 2>&1; then
    echo "Pulling $IMAGE..."
    docker pull "$IMAGE"
fi

# First run — create docker-compose.yml with admin credentials
if [ ! -f docker-compose.yml ]; then
    echo ""
    echo "============================================"
    echo "  Rampart File Manager — First Time Setup"
    echo "============================================"
    echo ""

    # Prompt for admin credentials
    printf "Admin username: "
    read ADMIN_USER
    if [ -z "$ADMIN_USER" ]; then
        echo "Username cannot be empty."
        exit 1
    fi

    # Read password with confirmation
    stty -echo 2>/dev/null || true
    printf "Admin password: "
    read ADMIN_PASS
    echo ""
    printf "Confirm password: "
    read ADMIN_PASS2
    echo ""
    stty echo 2>/dev/null || true

    if [ -z "$ADMIN_PASS" ]; then
        echo "Password cannot be empty."
        exit 1
    fi
    if [ "$ADMIN_PASS" != "$ADMIN_PASS2" ]; then
        echo "Passwords do not match."
        exit 1
    fi

    cat > docker-compose.yml << EOF
services:
  rampart:
    image: rampart-filemanager
    ports:
      - "8088:8088"
    volumes:
      - ./data:/app/data
      - ./web_server_conf.js:/app/web_server_conf.js
      - ./admin.sh:/app/admin.sh
    environment:
      - RAMPART_DOCKER=1
      - OO_JWT_SECRET=your-secret-key-change-me
      - OO_HASH=0
      - ADMIN_USER=${ADMIN_USER}
      - ADMIN_PASS=${ADMIN_PASS}
    cap_add:
      - SYS_ADMIN
    devices:
      - /dev/fuse
    security_opt:
      - apparmor:unconfined
    restart: unless-stopped

  onlyoffice:
    image: onlyoffice/documentserver
    environment:
      - JWT_SECRET=your-secret-key-change-me
      - SECURE_LINK_SECRET=verysecretstring
      - HASH=0
    restart: unless-stopped
EOF

    echo ""
    echo "Created docker-compose.yml"
fi

# Extract default files from the image if they don't exist locally
extract_file() {
    local src="$1" dst="$2"
    if [ ! -f "$dst" ]; then
        echo "Extracting $dst from image..."
        docker run --rm --entrypoint cat "$IMAGE" "$src" > "$dst"
        chmod +x "$dst" 2>/dev/null || true
    fi
}

extract_file /app/web_server_conf.js web_server_conf.js
extract_file /app/admin.sh admin.sh
mkdir -p data

# Start containers
echo "Starting containers..."
docker compose up -d

# Wait for the file manager to be reachable
echo ""
echo "Waiting for Rampart File Manager to start..."
echo "  (This typically takes 1-3 minutes on first start while ONLYOFFICE initializes)"
echo ""
TRIES=0
MAX_TRIES=300
while [ $TRIES -lt $MAX_TRIES ]; do
    STATUS=$(curl -sf --max-time 2 http://127.0.0.1:8088/filemanager/ > /dev/null 2>&1 && echo "ok" || true)
    if [ "$STATUS" = "ok" ]; then
        echo ""
        echo "============================================"
        echo "  Rampart File Manager is ready!"
        echo "  http://localhost:8088/filemanager/"
        echo "============================================"
        exit 0
    fi
    TRIES=$((TRIES + 1))
    if [ $((TRIES % 10)) -eq 0 ]; then
        echo "  Still starting... ($TRIES seconds)"
    fi
    sleep 1
done

echo ""
echo "WARNING: Server did not respond after $MAX_TRIES seconds."
echo "Check logs with: docker compose logs rampart"
exit 1
