#!/bin/bash
#
# Build the Rampart File Manager Docker image.
# Run from the docker/ directory.
#
# Usage:
#   ./build.sh                    # build for local platform only
#   ./build.sh --push             # build for amd64+arm64 and push to registry
#

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$SCRIPT_DIR/build"
CONF_FILE="$SCRIPT_DIR/.build.conf"
REPO="rampart-filemanager"
VERSION="0.1.0"

MODULES=(
    rampart-crypto.so
    rampart-curl.so
    rampart-lmdb.so
    rampart-net.so
    rampart-server.so
    rampart-webserver.js
    rampart-cmark.so
    rampart-html.so
    rampart-totext.so
    rampart-sql.so
)

# Load saved config if it exists
RAMPART_AMD64=""
RAMPART_ARM64=""
if [ -f "$CONF_FILE" ]; then
    . "$CONF_FILE"
fi

# Prompt for a Rampart path, with default
# Usage: prompt_path "label" "default" -> sets REPLY
prompt_path() {
    local label="$1" default="$2"
    if [ -n "$default" ]; then
        printf "%s [%s]: " "$label" "$default"
    else
        printf "%s (or 'skip'): " "$label"
    fi
    read REPLY
    if [ -z "$REPLY" ]; then
        REPLY="$default"
    fi
}

# Copy rampart distribution from a local or remote path into build context
# Usage: copy_rampart <source_path> <dest_dir>
copy_rampart() {
    local src="$1" dst="$2"
    mkdir -p "$dst/bin" "$dst/modules" "$dst/include"

    if echo "$src" | grep -q ':'; then
        # Remote path (user@host:/path)
        echo "  Copying from remote $src ..."
        rsync -a "$src/bin/rampart" "$dst/bin/"
        rsync -a "$src/bin/iroh-webproxy" "$dst/bin/" 2>/dev/null || true
        rsync -a "$src/include/" "$dst/include/"
        for mod in "${MODULES[@]}"; do
            rsync -a "$src/modules/$mod" "$dst/modules/" 2>/dev/null || true
        done
    else
        # Local path
        echo "  Copying from $src ..."
        cp "$src/bin/rampart" "$dst/bin/"
        cp "$src/bin/iroh-webproxy" "$dst/bin/" 2>/dev/null || true
        cp -r "$src/include/"* "$dst/include/"
        for mod in "${MODULES[@]}"; do
            [ -e "$src/modules/$mod" ] && cp "$src/modules/$mod" "$dst/modules/"
        done
    fi
}

# --- Prompt for Rampart paths ---
echo ""
echo "=== Rampart File Manager Docker Build ==="
echo ""

prompt_path "Path to x86_64 (amd64) Rampart installation" "${RAMPART_AMD64:-/usr/local/rampart}"
RAMPART_AMD64="$REPLY"

prompt_path "Path to ARM64 Rampart installation" "${RAMPART_ARM64:-}"
RAMPART_ARM64="$REPLY"

# Save config for next time
cat > "$CONF_FILE" << EOF
RAMPART_AMD64="$RAMPART_AMD64"
RAMPART_ARM64="$RAMPART_ARM64"
EOF

# --- Prepare build context ---
echo ""
echo "Preparing build context..."
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR/app"

# Determine platforms to build
PLATFORMS="linux/amd64"

if [ -n "$RAMPART_AMD64" ] && [ "$RAMPART_AMD64" != "skip" ]; then
    copy_rampart "$RAMPART_AMD64" "$BUILD_DIR/rampart-amd64"
else
    echo "ERROR: amd64 Rampart path is required."
    exit 1
fi

if [ -n "$RAMPART_ARM64" ] && [ "$RAMPART_ARM64" != "skip" ]; then
    copy_rampart "$RAMPART_ARM64" "$BUILD_DIR/rampart-arm64"
    PLATFORMS="linux/amd64,linux/arm64"
fi

# Copy graphicsmagick module source
cp -r "$APP_DIR/../graphicsmagick" "$BUILD_DIR/graphicsmagick"

# Copy app files (exclude runtime data, docker dir, git)
rsync -a --exclude='data/' \
         --exclude='docker/' \
         --exclude='logs/' \
         --exclude='.git/' \
         --exclude='*.pid' \
         --exclude='iroh-*' \
         --exclude='wsc.js' \
         --exclude='onlyoffice-pristine/' \
         "$APP_DIR/" "$BUILD_DIR/app/"

# Copy entrypoint and admin scripts
cp "$SCRIPT_DIR/entrypoint.sh" "$BUILD_DIR/entrypoint.sh"
cp "$SCRIPT_DIR/admin_source.sh" "$BUILD_DIR/admin.sh"

# --- Build ---
echo ""
echo "Building for: $PLATFORMS"

if [ "$1" = "--push" ]; then
    # Multi-platform build and push to registry
    docker buildx build \
        --platform "$PLATFORMS" \
        -t "$REPO:latest" \
        -t "$REPO:$VERSION" \
        -f "$SCRIPT_DIR/Dockerfile" \
        --push \
        "$BUILD_DIR"
elif [ "$1" = "--test" ]; then
    # Multi-platform build (verify both platforms compile, no push)
    docker buildx build \
        --platform "$PLATFORMS" \
        -t "$REPO:$VERSION" \
        -f "$SCRIPT_DIR/Dockerfile" \
        "$BUILD_DIR"
else
    # Local build (current platform only, loads into docker)
    docker build -t "$REPO:latest" -t "$REPO:$VERSION" -f "$SCRIPT_DIR/Dockerfile" "$BUILD_DIR"
fi

echo ""
echo "Cleaning up build context..."
rm -rf "$BUILD_DIR"

echo "Done."
if [ "$1" != "--push" ]; then
    echo "Run with: ./start.sh"
fi
