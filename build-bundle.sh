#!/bin/bash
# build-bundle.sh — package the file-manager server as a single self-contained
# executable: rampart binary + appended zip of entry_script + html/apps/wsapps.
#
# Usage:
#   ./build-bundle.sh                       # → ./rampart-filemanager
#   ./build-bundle.sh /path/to/out          # → /path/to/out
#   RAMPART=/path/to/rampart ./build-bundle.sh
set -euo pipefail

SRC_DIR="$(cd "$(dirname "$0")" && pwd)"

# Output layout:
#   $OUT_BASE       — shell wrapper (what end users invoke)
#   $OUT_BASE.bin   — raw self-extracting bundle (rampart + zip payload)
# The wrapper carries the .bin appended after a unique marker.  On first run
# it extracts the .bin alongside itself, removes macOS quarantine if present,
# truncates itself to drop the appended payload, then execs the .bin.
OUT_BASE="${1:-$SRC_DIR/rampart-filemanager}"
OUT_BASE="${OUT_BASE%.bin}"
OUT_BIN="${OUT_BASE}.bin"
OUT_WRAPPER="$OUT_BASE"

# Locate rampart: explicit RAMPART= wins; otherwise ask rampart itself for the
# canonical install path (process.installPathBin) so this works on any system
# regardless of where rampart was installed.
if [ -n "${RAMPART:-}" ]; then
    if [ ! -x "$RAMPART" ]; then
        echo "rampart binary not found at \$RAMPART=$RAMPART" >&2
        exit 1
    fi
else
    if ! command -v rampart >/dev/null 2>&1; then
        echo "rampart not in PATH. Install it, or set RAMPART=/path/to/rampart." >&2
        exit 1
    fi
    INSTALL_BIN="$(rampart -c 'rampart.utils.printf("%s", process.installPathBin)' 2>/dev/null || true)"
    if [ -z "$INSTALL_BIN" ] || [ ! -x "$INSTALL_BIN/rampart" ]; then
        echo "Could not derive rampart binary location (process.installPathBin returned: '$INSTALL_BIN')." >&2
        exit 1
    fi
    RAMPART="$INSTALL_BIN/rampart"
fi

# Rampart modules to embed in the bundle. For each name we copy:
#   <process.modulesPath>/<name>.so   (native module, if present)
#   <process.modulesPath>/<name>.js   (JS shim, if present)
# Add or remove names below to control which rampart modules ship with the
# bundle. Names are bare module identifiers (what you'd pass to require()).
MODULES=(
    rampart-almanac         # date / time
    rampart-cmark           # markdown
    rampart-crypto          # passwords, hashing
    rampart-curl            # http client
    rampart-gm              # graphicsmagick JS shim
    rampart-graphicsmagick  # graphicsmagick native (thumbnails)
    rampart-lmdb            # database
    rampart-net             # network
    rampart-server          # http server
    rampart-sql             # full-text index / vector
    rampart-sqlUpdate       # scheduled fulltext index updates (used by webdav.js)
    rampart-totext          # extract text from documents
    rampart-webserver       # web_server_conf launcher
)

MODULES_PATH="$(rampart -c 'rampart.utils.printf("%s", process.modulesPath)' 2>/dev/null || true)"
if [ -z "$MODULES_PATH" ] || [ ! -d "$MODULES_PATH" ]; then
    echo "Could not derive rampart modules path (process.modulesPath returned: '$MODULES_PATH')." >&2
    exit 1
fi
if [ ! -f "$SRC_DIR/entry_script.js" ]; then
    echo "Missing entry_script.js in $SRC_DIR" >&2
    exit 1
fi
for d in apps html wsapps; do
    if [ ! -d "$SRC_DIR/$d" ]; then
        echo "Missing source directory: $SRC_DIR/$d" >&2
        exit 1
    fi
done

STAGE="$(mktemp -d -t rfm-bundle.XXXXXX)"
trap 'rm -rf "$STAGE"' EXIT

echo "Staging into $STAGE ..."
cp "$SRC_DIR/entry_script.js" "$STAGE/"

# Copy assets, preserving permissions and symlinks
cp -a "$SRC_DIR/apps"   "$STAGE/"
cp -a "$SRC_DIR/html"   "$STAGE/"
cp -a "$SRC_DIR/wsapps" "$STAGE/"

# Copy each requested rampart module (both .so and .js variants where they
# exist). Native modules in the zip are auto-extracted to /tmp at load time.
# JS modules stay at the zip root and resolve via require() normally.
echo "Copying rampart modules from $MODULES_PATH ..."
missing=()
for mod in "${MODULES[@]}"; do
    found=0
    for ext in .so .js; do
        src="$MODULES_PATH/$mod$ext"
        if [ -f "$src" ]; then
            cp -a "$src" "$STAGE/"
            found=1
        fi
    done
    if [ "$found" -eq 0 ]; then
        missing+=("$mod")
    fi
done
if [ "${#missing[@]}" -gt 0 ]; then
    echo "Warning: requested modules not found in $MODULES_PATH:" >&2
    printf '  %s\n' "${missing[@]}" >&2
fi

# Helper binaries that rampart's native modules auto-extract from the zip
# at runtime.  texislockd is the SQL/vector-index lock daemon — without it
# in the bundle, rampart-sql.so fails when it tries to spawn the lock daemon.
RAMPART_BIN_DIR="$(dirname "$RAMPART")"
HELPERS=( texislockd )
for h in "${HELPERS[@]}"; do
    src="$RAMPART_BIN_DIR/$h"
    if [ -f "$src" ]; then
        echo "Copying helper binary $h ..."
        cp -a "$src" "$STAGE/"
    else
        echo "Warning: helper binary $h not found at $src" >&2
    fi
done

# Strip noisy editor/scratch files so the bundle stays lean
find "$STAGE" \( \
        -name '*.bak'  -o -name '*~'  -o -name '.DS_Store' -o \
        -name '*.swp'  -o -name '.git' -o -name '.gitkeep' -o \
        -name '*.log' \
    \) -prune -exec rm -rf {} +

# Build the zip — store everything at the zip root so :zip:/entry_script.js
# resolves, and rampart-<mod>.so/.js are findable by require().
echo "Zipping payload ..."
( cd "$STAGE" && zip -qr "$STAGE/payload.zip" . -x 'payload.zip' )

# Glue — assemble the raw .bin first
echo "Concatenating onto $RAMPART ..."
cp "$RAMPART" "$OUT_BIN"
cat "$STAGE/payload.zip" >> "$OUT_BIN"
chmod +x "$OUT_BIN"

# Now build the self-extracting wrapper.  The wrapper script ends with a
# marker line, after which the .bin contents are appended verbatim.
echo "Building wrapper $OUT_WRAPPER ..."
WRAPPER_TMP="$STAGE/wrapper.sh"
cat > "$WRAPPER_TMP" <<'WRAPPER_EOF'
#!/usr/bin/env bash
# rampart-filemanager — self-extracting wrapper.
#
# First run: extracts the embedded bundle as <self>.bin, drops macOS
# quarantine xattr if present, truncates this script to remove the
# appended payload (so subsequent runs skip extraction), then execs
# the .bin with all forwarded arguments.
#
# Subsequent runs: just exec <self>.bin directly.

set -e

# Resolve our own absolute path without depending on readlink -f (BSD-safe)
SELF="$0"
case "$SELF" in /*) ;; *) SELF="$PWD/$SELF" ;; esac
SELF_DIR="$(cd "$(dirname "$SELF")" && pwd)"
SELF="$SELF_DIR/$(basename "$SELF")"
BIN="${SELF}.bin"

# Intercept -h/--help *before* extraction or exec.  Without this, --help would
# fall through to rampart's built-in help (which is about the rampart runtime,
# not this file-manager bundle) and bypass our setup wizard entirely.
for _arg in "$@"; do
    case "$_arg" in
        -h|--help|-\?|--\?)
            _name="$(basename "$SELF")"
            cat <<USAGE
$_name — bundled Rampart File Manager server

Usage:
  $_name [options] [command]

A self-contained WebDAV file manager + viewer.  Ships rampart, the file
manager app, and the rampart modules it needs in a single executable.

First invocation:
  Prompts interactively for data/log directory, HTTP or HTTPS, port, the
  drop-to user if started as root, and the first administrator account.
  Writes filemanager-conf.js alongside this binary and starts the
  server.  Extracts the inner binary as ${_name}.bin and removes itself
  as the wrapper layer afterward.

Subsequent invocations:
  Loads filemanager-conf.js and starts the server with those settings,
  overridden by any command-line flags below.

Commands:
  start                     Start the server (default)
  stop                      Stop a running server (matches the pid file)
  restart                   stop then start
  status                    Print whether a server is running on this config
  letssetup                 HTTP-only on port 80 for Let's Encrypt ACME challenge

Common options (passed through to rampart-webserver, override saved config):
  --port N                  Listen on port N (both ipv4 and ipv6)
  --ipPort N                IPv4 port only
  --ipv6Port N              IPv6 port only
  --bindAll                 Bind 0.0.0.0 and [::]
  --ipAddr ADDR             Bind a specific IPv4 address (disables bindAll)
  --ipv6Addr ADDR           Bind a specific IPv6 address
  --user NAME               If started as root, drop privileges to this user
  --secure                  Enable HTTPS
  --sslKeyFile PATH         TLS private key file (with --secure)
  --sslCertFile PATH        TLS certificate file (with --secure)
  --selfSign                Generate a self-signed cert on first start (with --secure)
  --letsencrypt HOST        Issue/renew Let's Encrypt cert for HOST (with --secure)
  --redirPort N             Run an HTTP→HTTPS redirector on port N
  --logRoot DIR             Log directory (default: <dataDir>/logs)
  --dataRoot DIR            LMDB/uploads directory (default: <dataDir>/data)
  --daemon                  Run in background (default: true)
  --no-daemon               Run in foreground
  -h, --help                Show this message and exit
  --lsopts                  Print every option rampart-webserver accepts

To reconfigure from scratch:
  Delete filemanager-conf.js (next to this binary) and re-run.

Files (relative to this binary's directory):
  filemanager-conf.js    Saved server configuration
  ${_name}.bin    Extracted bundle (created on first run)

USAGE
            exit 0
            ;;
    esac
done

# Subsequent run — binary already extracted
if [ -x "$BIN" ]; then
    exec "$BIN" "$@"
fi

# Assemble the marker at runtime so the literal string only appears once in
# this file — at the actual end-of-script marker line — making grep robust.
MARKER="### __RFM_PAYLOAD"
MARKER="${MARKER}_FOLLOWS__ ###"

# Find the marker line's byte offset within this file
PAYLOAD_OFFSET="$(grep -aboF -m1 -- "$MARKER" "$SELF" | head -1 | cut -d: -f1 || true)"
if [ -z "$PAYLOAD_OFFSET" ]; then
    echo "$SELF: payload marker not found in self; bundle is corrupted." >&2
    exit 1
fi

# Bytes consumed by the marker line itself = marker text + trailing newline
HEADER_BYTES=$(( PAYLOAD_OFFSET + ${#MARKER} + 1 ))

echo "First run: extracting $BIN ..." >&2
# tail -c +N starts at byte N (1-indexed), portable across Linux and macOS.
# Write to a temp first so a half-finished extraction (disk full, ^C) doesn't
# leave a corrupt .bin that the subsequent-run shortcut would happily exec.
BIN_TMP="${BIN}.tmp.$$"
tail -c +"$(( HEADER_BYTES + 1 ))" "$SELF" > "$BIN_TMP"
chmod +x "$BIN_TMP"
mv "$BIN_TMP" "$BIN"

# macOS Gatekeeper marks downloaded files with com.apple.quarantine; remove
# it so the binary can exec without a confirmation prompt.
if [ "$(uname)" = "Darwin" ]; then
    xattr -d com.apple.quarantine "$BIN" 2>/dev/null || true
fi

# Truncate self to keep only bytes up to and including the marker line, so
# subsequent invocations short-circuit to the `if [ -x "$BIN" ]` exec above.
TMP="${SELF}.tmp.$$"
head -c "$HEADER_BYTES" "$SELF" > "$TMP"
mv "$TMP" "$SELF"
chmod +x "$SELF"

exec "$BIN" "$@"

### __RFM_PAYLOAD_FOLLOWS__ ###
WRAPPER_EOF

# Combine: wrapper script + appended .bin contents
cat "$WRAPPER_TMP" "$OUT_BIN" > "$OUT_WRAPPER"
chmod +x "$OUT_WRAPPER"

BIN_SIZE="$(du -h "$OUT_BIN"     | cut -f1)"
WRP_SIZE="$(du -h "$OUT_WRAPPER" | cut -f1)"
echo
echo "Built:"
echo "  $OUT_BIN      ($BIN_SIZE)  — raw self-extracting bundle"
echo "  $OUT_WRAPPER  ($WRP_SIZE)  — wrapper (extracts .bin on first run; unquarantines on macOS)"
echo
echo "Distribute the wrapper. First run prompts for a data directory and"
echo "admin credentials. Run as root for ports < 1024, regular user otherwise."
