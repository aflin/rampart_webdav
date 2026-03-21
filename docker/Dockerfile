ARG TARGETARCH

FROM frolvlad/alpine-glibc AS builder

# Build dependencies for rampart-gm.so
RUN apk add --no-cache build-base graphicsmagick-dev bsd-compat-headers freetype-dev bzip2-dev zlib-dev

ARG TARGETARCH
COPY rampart-${TARGETARCH}/include/ /usr/local/rampart/include/
COPY graphicsmagick/ /build/graphicsmagick/

WORKDIR /build/graphicsmagick
RUN make clean && make

# --- Final image ---
FROM frolvlad/alpine-glibc

# Runtime dependencies
RUN apk add --no-cache ffmpeg ca-certificates rclone fuse3 graphicsmagick bash curl \
    && echo 'user_allow_other' >> /etc/fuse.conf

# Copy Rampart distribution (platform-specific)
ARG TARGETARCH
COPY rampart-${TARGETARCH}/ /usr/local/rampart/
RUN ln -s /usr/local/rampart/bin/rampart /usr/local/bin/rampart

# Replace host-built rampart-gm.so with Alpine-built one
COPY --from=builder /build/graphicsmagick/rampart-gm.so /usr/local/rampart/modules/

# Copy app
COPY app/ /app/

# Create app user with home directory (needed for rclone config)
RUN adduser -D -h /home/rampart rampart

# Data directory (mount a volume here for persistence)
RUN mkdir -p /app/data /app/logs && chown -R rampart:rampart /app

WORKDIR /app

EXPOSE 8088

COPY entrypoint.sh /entrypoint.sh
COPY admin.sh /app/admin.sh
RUN cp /app/web_server_conf.js /app/web_server_conf.js.default \
    && chmod +x /app/admin.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
