# Using ONLYOFFICE with Rampart File Manager Behind Nginx

This guide covers setting up the Rampart File Manager with ONLYOFFICE Document Server when Nginx handles SSL termination and proxies requests to Rampart.

## Architecture

```
Browser --HTTPS--> Nginx:443 --HTTP--> Rampart:8088 --HTTP--> ONLYOFFICE:6659
                                                 <--HTTP--
```

- **Nginx** terminates SSL and proxies to Rampart
- **Rampart** serves the file manager and proxies ONLYOFFICE routes
- **ONLYOFFICE** runs in Docker, accessed by Rampart on localhost

## Prerequisites

- Nginx with SSL configured (e.g., Let's Encrypt)
- Rampart web server running on a local port (e.g., 8088)
- ONLYOFFICE Docker container running (e.g., port 6659)
- Docker installed with `docker compose`

## Step 1: ONLYOFFICE Docker Setup

Create `onlyoffice/docker-compose.yml`:

```yaml
services:
  documentserver:
    image: onlyoffice/documentserver
    container_name: onlyoffice
    ports:
      - "6659:80"
    environment:
      - JWT_SECRET=your-secret-key-change-me
      - SECURE_LINK_SECRET=verysecretstring
      - HASH=0
    extra_hosts:
      - "host.docker.internal:host-gateway"
    restart: unless-stopped
```

Start it:

```bash
docker compose up -d
```

Wait 2-5 minutes for ONLYOFFICE to initialize, then verify:

```bash
curl http://127.0.0.1:6659/healthcheck
# Should return: true
```

## Step 2: Rampart Configuration

In your `web_server_conf.js` (or equivalent), add the callback host override **before** the ONLYOFFICE detection section:

```javascript
// Set this to your public hostname so ONLYOFFICE callbacks
// go through Nginx (required when behind a reverse proxy)
var onlyOfficeCallbackHost = 'example.com';
```

This is necessary because ONLYOFFICE needs to call back to Rampart to fetch and save documents. Without this setting, it tries to use `host.docker.internal` which often fails behind a reverse proxy due to:
- Firewalls blocking Docker bridge traffic to the host
- The hostname not resolving correctly inside the container
- SSL certificate mismatches

## Step 3: Docker Networking and Firewalls

ONLYOFFICE runs in a Docker container and needs to reach the host machine. Common issues:

### Firewall Blocking Docker Traffic

Docker creates bridge networks (typically `172.16.0.0/12`). If your host firewall blocks traffic from these subnets, ONLYOFFICE can't reach Rampart.

**Check your Docker networks:**
```bash
docker network inspect bridge | grep Subnet
```

**Add Docker subnets to your firewall's trusted list:**
```bash
# iptables — insert at the TOP of the INPUT chain
iptables -I INPUT 1 -s 172.16.0.0/12 -j ACCEPT
```

Make this permanent in your firewall script. The range `172.16.0.0/12` covers all Docker networks (`172.16.x.x` through `172.31.x.x`).

### Docker Service Must Start After Firewall

Docker sets up NAT/MASQUERADE rules in iptables when it starts. If your firewall script runs after Docker and flushes iptables, Docker's networking breaks.

**Solution:** Either:
1. Run your firewall script before Docker starts, or
2. Restart Docker after running the firewall script: `service docker restart`

### Verify Connectivity

From inside the ONLYOFFICE container:
```bash
# Should return "Authentication required" quickly
docker exec onlyoffice curl -m 3 http://host.docker.internal:8088/dav/
```

If this times out, the firewall is blocking it.

## Step 4: Nginx Configuration

### Upstream Definition

```nginx
upstream rampart {
    server 127.0.0.1:8088;
}
```

### Hash Size (prevents warnings)

You may need to add to your `http` block:

```nginx
proxy_headers_hash_max_size 1024;
proxy_headers_hash_bucket_size 128;
```

### Proxy Locations

Add all of the following inside your `server` block. Every location needs `X-Forwarded-Proto` so ONLYOFFICE generates HTTPS URLs (preventing mixed content errors).

```nginx
    # WebDAV endpoint — file operations
    location /dav/ {
        proxy_set_header Host $host;
        proxy_set_header Remote_addr $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_pass http://rampart;
        client_max_body_size 0;
        proxy_read_timeout 300;
    }

    # File manager static files
    # Change /filemanager/ if your installation uses a different path
    location /filemanager/ {
        proxy_set_header Host $host;
        proxy_set_header Remote_addr $remote_addr;
        proxy_pass http://rampart;
    }

    # WebSocket apps (SSH terminal, VNC)
    location /wsapps/terminal/ {
        proxy_set_header Host $host;
        proxy_set_header Remote_addr $remote_addr;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_read_timeout 3600;
        proxy_pass http://rampart;
    }
    location /wsapps/vnc/ {
        proxy_set_header Host $host;
        proxy_set_header Remote_addr $remote_addr;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_read_timeout 3600;
        proxy_pass http://rampart;
    }

    # ONLYOFFICE static assets
    location /web-apps/ {
        proxy_set_header Host $host;
        proxy_set_header Remote_addr $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_pass http://rampart;
    }
    location /sdkjs/ {
        proxy_set_header Host $host;
        proxy_set_header Remote_addr $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_pass http://rampart;
    }
    location /sdkjs-plugins/ {
        proxy_set_header Host $host;
        proxy_set_header Remote_addr $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_pass http://rampart;
    }
    location /fonts/ {
        proxy_set_header Host $host;
        proxy_set_header Remote_addr $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_pass http://rampart;
    }
    location /dictionaries/ {
        proxy_set_header Host $host;
        proxy_set_header Remote_addr $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_pass http://rampart;
    }
    location /cache/ {
        proxy_set_header Host $host;
        proxy_set_header Remote_addr $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_pass http://rampart;
    }
    location /healthcheck {
        proxy_set_header Host $host;
        proxy_set_header Remote_addr $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_pass http://rampart;
    }

    # ONLYOFFICE document collaboration (requires WebSocket)
    location /doc/ {
        proxy_set_header Host $host;
        proxy_set_header Remote_addr $remote_addr;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_read_timeout 3600;
        proxy_pass http://rampart;
    }
    location /coauthoring/ {
        proxy_set_header Host $host;
        proxy_set_header Remote_addr $remote_addr;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_pass http://rampart;
    }

    # ONLYOFFICE version-prefixed assets (e.g. /9.3.1-0/)
    # This regex matches the version prefix that ONLYOFFICE uses
    location ~ ^/\d+\.\d+\.\d+-\d+/ {
        proxy_set_header Host $host;
        proxy_set_header Remote_addr $remote_addr;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_read_timeout 3600;
        proxy_pass http://rampart;
    }
```

### Test and Reload

```bash
nginx -t && nginx -s reload
```

## Step 5: Restart Rampart

```bash
rampart web_server_conf.js restart
```

Verify ONLYOFFICE is detected:
```
ONLYOFFICE ready (version prefix: /9.3.1-0/, port: 6659).
```

## Troubleshooting

### "Download failed" when opening a document

ONLYOFFICE can't fetch the document from Rampart.

**Check the callback URL:**
```bash
docker logs onlyoffice 2>&1 | grep -i "download\|fetch\|error" | tail -10
```

- If it shows `host.docker.internal` timing out: set `onlyOfficeCallbackHost` in the server config
- If it shows your hostname with a connection error: check firewall rules

### Mixed content errors in browser console

ONLYOFFICE is generating `http://` URLs on an `https://` page.

**Fix:** Ensure all Nginx proxy locations include:
```nginx
proxy_set_header X-Forwarded-Proto $scheme;
```

### WebSocket connection refused

The browser can't establish WebSocket connections for real-time editing.

**Fix:** Ensure these locations have WebSocket upgrade headers:
- `/doc/`
- `/coauthoring/`
- The version-prefix regex (`/9.3.1-0/...`)

```nginx
proxy_set_header Upgrade $http_upgrade;
proxy_set_header Connection "upgrade";
proxy_http_version 1.1;
proxy_read_timeout 3600;
```

### 502 Bad Gateway

Rampart can't reach ONLYOFFICE.

**Check:**
```bash
# Is ONLYOFFICE running?
docker ps | grep onlyoffice

# Can Rampart reach it?
curl http://127.0.0.1:6659/healthcheck

# Is it being properly proxied through rampart?
curl http://127.0.0.1:8088/healthcheck

# If all else fails, stop all three services then restart everything: 
# onlyoffice docker, then rampart webserver, then nginx.
```

If the healthcheck fails, ONLYOFFICE may still be starting (wait 2-5 minutes) or may have crashed (`docker logs onlyoffice`).
Or use the supplied onlyoffice/setup.sh

### Nginx warning about proxy_headers_hash

```
nginx: [warn] could not build optimal proxy_headers_hash...
```

**Fix:** Add to your `http` block:
```nginx
proxy_headers_hash_max_size 1024;
proxy_headers_hash_bucket_size 128;
```

### ONLYOFFICE container can't resolve hostname

```bash
# Verify hostname resolution inside container
docker exec onlyoffice getent hosts host.docker.internal

# If using a custom callback host, verify it resolves
docker exec onlyoffice curl -m 3 https://example.com/dav/
```

If using `onlyOfficeCallbackHost` with your public hostname and it times out, the container may need to reach the host through Docker networking. Add to docker-compose.yml:

```yaml
    extra_hosts:
      - "example.com:host-gateway"
```

This maps your hostname to the Docker host gateway IP inside the container.

## Summary of Configuration Changes

| File | Setting | Purpose |
|------|---------|---------|
| `web_server_conf.js` | `var onlyOfficeCallbackHost = 'example.com'` | Route callbacks through Nginx |
| `docker-compose.yml` | `extra_hosts: host.docker.internal:host-gateway` | Docker-to-host name resolution |
| `nginx.conf` | `proxy_set_header X-Forwarded-Proto $scheme` | Prevent mixed content errors |
| `nginx.conf` | WebSocket headers on `/doc/`, `/coauthoring/`, version prefix | Real-time collaborative editing |
| `nginx.conf` | `client_max_body_size 0` on `/dav/` | Allow large file uploads |
| Firewall | Allow `172.16.0.0/12` in INPUT chain | Docker container access to host |
