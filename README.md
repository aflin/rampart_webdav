# Rampart File Manager

A self-hosted web file manager with a full-featured WebDAV backend, built on the [Rampart](https://github.com/aflin/rampart) web server.

## Features

### File Manager UI

- **Detail and grid views** with sortable columns (name, size, type, date, owner, permissions)
- **Thumbnail generation** for images and videos (requires ffmpeg for video thumbnails)
- **Drag-and-drop** file upload from desktop and internal move/copy between folders
- **URL drop** — drag a URL onto the file panel to save web pages or download videos (via plugins)
- **Clipboard paste** for quick file uploads
- **Chunked uploads** for large files with progress indicators
- **File previews and viewers:**
  - Images — full viewer with slideshow, zoom, and navigation between files
  - Video — Video.js player with subtitle support
  - Audio — waveform player with playlist builder and M3U support
  - PDF — in-browser viewer
  - EPUB — e-book reader with table of contents
  - Code/text — CodeMirror editor with syntax highlighting and configurable themes
  - HTML — WYSIWYG editor (Jodit) with source mode toggle
  - CSV/TSV — interactive table editor with sorting, cell editing, and save
  - Office documents — ONLYOFFICE integration for spreadsheets, documents, and presentations (optional, see below)
- **Image editor** — Filerobot for cropping, filters, annotations, and export
- **File information panel** with metadata, permissions, ownership, and share link management
- **Public share links** with optional expiration for files and directories
- **Context menu** with rename, copy, move, delete, download, zip, and permissions
- **Multi-select** with bulk operations
- **Keyboard navigation** and shortcuts
- **Back/Forward/Refresh** navigation buttons
- **Search** within the current directory
- **Recycle bin** (trash) per user

### Terminal

- **Browser-based terminal** via xterm.js with WebSocket connection
- **SSH client** — connect to remote hosts directly from the browser
- Configurable color themes (dark, light, custom JSON)
- Enabled per-user by admin

### VNC Remote Desktop (Experimental)

- **Browser-based VNC client** via noVNC with WebSocket-to-TCP bridge
- Connect to any VNC server (TigerVNC, x11vnc, QEMU, macOS Screen Sharing)
- Username/password authentication (supports Apple Remote Desktop)
- Scaling and resize support
- Enabled per-user by admin
- Note: Some VNC servers may use encodings that are not fully supported
- **Warning:** The connection between Rampart and the VNC server is unencrypted. Screen data and keystrokes are sent in plaintext over TCP. This is safe when the VNC server is on the same machine as Rampart, but connections to remote VNC servers should be tunneled through SSH or a VPN.

### Plugin System

- **File viewer/editor plugins** — open files with custom renderers
- **URL drop handler plugins** — process dropped URLs (web clipper, video downloader)
- Background job support for long-running downloads with progress display
- Plugins loaded automatically from `apps/webdav/plugins/`
- See [PLUGINS.md](apps/webdav/plugins/PLUGINS.md) for plugin development docs

### User Management

- Multi-user with per-user home directories
- Admin panel for creating, deleting, and managing users
- Groups and per-file Unix-style permissions (owner/group/other, read/write)
- Cookie-based sessions with configurable timeout
- Password management with forced password change on first login

### Cloud Storage

- Requires [rclone](https://rclone.org/install/) installed on the server (a recent version is required — v1.53+ tested).
  - May require the `fuse3` package (`apt install fuse3` or equivalent)
  - If rclone mounts are not visible to the web server, uncomment `user_allow_other` in `/etc/fuse.conf`

- **Google Drive, Dropbox, OneDrive** — OAuth-based mounting
- **Amazon S3, Backblaze B2, Wasabi, MinIO** — key-based mounting
- **SFTP** — password-encrypted credential storage, auto-remount on login
- **Manual configuration** — any rclone-supported provider
- Mounts appear as folders in the user's home directory or at the root level (admin)

### WebDAV Backend

- **RFC 4918 compliant, Class 2** with full locking support (LOCK/UNLOCK)
- All standard methods: OPTIONS, GET, HEAD, PUT, DELETE, MKCOL, COPY, MOVE, PROPFIND, PROPPATCH
- Compatible with external WebDAV clients (macOS Finder, Windows Explorer, Linux davfs2, Cyberduck, etc.)
- Dead property storage (PROPPATCH/PROPFIND custom properties)
- Symlink traversal protection

### Appearance

- Light, dark, and auto (system) themes
- Configurable code editor themes (20+ CodeMirror themes)
- Adjustable grid view sizing

### Demo Mode

Built-in demo mode for public showcases:

- Single `demo:demo` user account
- Read-only `demo-files` directory with sample files
- Automatic cleanup of user files after a configurable interval
- Upload size and storage quota limits
- Password change, session management, cloud storage, and admin features disabled
- Terminal and VNC access disabled

## Requirements

- [Rampart](https://github.com/aflin/rampart) v0.6.2 or later
- Optional: [ffmpeg](https://ffmpeg.org/) for video thumbnails
- Optional: [rclone](https://rclone.org/) for cloud storage mounting (v1.53+ recommended)
  - Requires `fuse3` package (`apt install fuse3` or equivalent)
  - If rclone mounts are not visible to the web server, uncomment `user_allow_other` in `/etc/fuse.conf`
- Optional: [Docker](https://www.docker.com/) for ONLYOFFICE document editing
- Optional: [yt-dlp](https://github.com/yt-dlp/yt-dlp) for video download plugin

## Quick Start

1. **Install Rampart** — see the [Rampart documentation](https://github.com/aflin/rampart) for installation instructions.

2. **Clone this repository:**
   ```bash
   git clone <repo-url> web_server
   cd web_server
   ```

3. **Configure the server** by editing `web_server_conf.js`:
   - Set `port` (default: 8088, use 443 for HTTPS)
   - Set `user` to the system user the server should run as
   - For HTTPS, configure `secure`, `sslKeyFile`/`sslCertFile`, `selfSign`, or `letsencrypt`
   - For demo mode, set `demoMode = true` and adjust `demoClearTime`, `demoMaxFileSize`, `demoMaxQuota`

4. **Create the admin user:**
   ```bash
   rampart apps/webdav/webdav.js add admin <password>
   rampart apps/webdav/webdav.js admin admin true
   ```
   This must be done before starting the server for the first time.

5. **Start the server:**
   ```bash
   rampart web_server_conf.js start
   ```

   If you use a port less than 1024, such as 80 or 443, the server must be started as root and `user` must be set in `web_server_conf.js`.

   ```bash
   sudo rampart web_server_conf.js start
   ```

   The filemanager will be at, e.g., http://localhost:8088/filemanager/.

   Enabling the secure server options in `web_server_conf.js` is highly recommended,
   especially for mounting it on your filesystem over the internet.

   To mount it with davfs2 or in the macOS Finder, use:
   https://yourserver.tld/dav/username and provide name and password.

6. **Manage the server:**
   ```bash
   rampart web_server_conf.js stop       # stop the server
   rampart web_server_conf.js restart    # restart the server
   rampart web_server_conf.js status     # check if running
   ```

7. **Command-line user administration** (can be used while the server is running):
   ```bash
   rampart apps/webdav/webdav.js add <username> <password>    # create a user
   rampart apps/webdav/webdav.js del <username>               # delete a user
   rampart apps/webdav/webdav.js list                         # list all users
   rampart apps/webdav/webdav.js passwd <username> <password>  # change password
   rampart apps/webdav/webdav.js admin <username> true|false   # set admin status
   ```

## ONLYOFFICE Document Editing (Optional)

ONLYOFFICE enables in-browser editing of Office documents (DOCX, XLSX, PPTX, ODS, etc.) with autosave support. It runs as a Docker container.

### Setup

1. **Install Docker** — see [Docker installation](https://docs.docker.com/engine/install/).

2. **Run the setup script:**
   ```bash
   cd onlyoffice
   sudo ./setup.sh
   ```

   The script will:
   - Pull the ONLYOFFICE Docker image (~1 GB download on first run)
   - Start the container and wait for it to become healthy (2-5 minutes)

3. **Restart the Rampart server** so it detects ONLYOFFICE:
   ```bash
   rampart web_server_conf.js restart
   ```

   On startup, Rampart will detect the ONLYOFFICE container, extract the JWT secret and version prefix from the Docker configuration, and add the necessary proxy routes automatically.

### Network Requirements

The ONLYOFFICE container needs to connect back to Rampart to fetch and save documents. Since Docker containers have their own network, `localhost` inside the container does not reach the host machine. To handle this:

- `docker-compose.yml` maps `host.docker.internal` to the host machine
- `web_server_conf.js` must have `bindAll: true` so Rampart listens on all interfaces (not just 127.0.0.1), allowing the container to reach it via the Docker bridge network

### How It Works

- Rampart proxies ONLYOFFICE endpoints (`/web-apps/`, `/cache/`, `/coauthoring/`, etc.) to the Docker container
- The JWT secret is read from `docker-compose.yml` at server startup — no hardcoded secrets
- The ONLYOFFICE version prefix is extracted from the running container, so upgrades are automatic
- If the Docker image is not installed, the server starts normally with document editing disabled
- If the container is stopped, Rampart will start it automatically on server startup
- Callback URLs always use `host.docker.internal` so ONLYOFFICE can reach Rampart regardless of how the browser connects (direct, proxy, tunnel)

### Reverse Proxy Setup

If Rampart runs behind a reverse proxy (e.g. nginx handling TLS), the proxy must forward these headers:

```nginx
proxy_set_header Host $host;
proxy_set_header X-Forwarded-Host $host;
proxy_set_header X-Forwarded-Proto $scheme;
proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
proxy_set_header Upgrade $http_upgrade;
proxy_set_header Connection "Upgrade";
```

### User Settings

Each user can toggle ONLYOFFICE autosave in Settings > Document Editor. When autosave is off, documents are saved only on Ctrl+S or when the editor save button is clicked.

## File Structure

```
web_server/
  web_server_conf.js       — Server configuration
  html/filemanager/        — Frontend (HTML, CSS, JavaScript)
  apps/webdav/             — WebDAV server module
  apps/webdav/plugins/     — File viewer and URL drop plugins
  wsapps/terminal/         — Terminal WebSocket bridge
  wsapps/vnc/              — VNC WebSocket-to-TCP bridge
  data/                    — User data, database, thumbnails (created at runtime)
  onlyoffice/              — ONLYOFFICE Docker configuration
    docker-compose.yml     — Container definition
    setup.sh               — Setup script
```

## License

MIT

## Third-Party Libraries

This project uses the following open-source libraries:

| Library | License | Website |
|---|---|---|
| [Rampart](https://github.com/aflin/rampart) | MIT | Web server and runtime |
| [CodeMirror 6](https://codemirror.net/) | MIT | Code/text editor |
| [Video.js](https://videojs.com/) | Apache-2.0 | Video player |
| [PDF.js](https://mozilla.github.io/pdf.js/) | Apache-2.0 | PDF renderer (Mozilla) |
| [Jodit](https://xdsoft.net/jodit/) | MIT | WYSIWYG HTML editor |
| [Filerobot Image Editor](https://github.com/scaleflex/filerobot-image-editor) | MIT | Image editor (Scaleflex) |
| [noVNC](https://novnc.com/) | MPL-2.0 | VNC client |
| [xterm.js](https://xtermjs.org/) | MIT | Terminal emulator |
| [epub.js](https://github.com/futurepress/epub.js) | BSD-2-Clause | EPUB reader |
| [JSZip](https://stuk.github.io/jszip/) | MIT / GPLv3 | ZIP library |
| [marked](https://marked.js.org/) | MIT | Markdown parser |
| [jsmediatags](https://github.com/aadsm/jsmediatags) | BSD-3-Clause | Audio metadata reader |
| [Ace Editor](https://ace.c9.io/) | BSD-3-Clause | Code editor (used by Jodit) |
| [js-beautify](https://github.com/beautify-web/js-beautify) | MIT | HTML formatter (used by Jodit) |
| [@uiw/codemirror-themes](https://github.com/uiwjs/react-codemirror) | MIT | CodeMirror themes |

See `html/js/local/LICENSES.md` for details when using locally hosted copies.
