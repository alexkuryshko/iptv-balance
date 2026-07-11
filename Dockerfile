FROM python:3.12-slim

LABEL org.opencontainers.image.title="IPTV Balance"
LABEL org.opencontainers.image.description="Auto-pick the best new.tv.team server, test speed (cabinet-style), proxy the playlist."
LABEL org.opencontainers.image.source="https://github.com/alexkuryshko/iptv-balance"

# Code (read-only)
WORKDIR /app
COPY server.py logo.png servers.json config.example.json ./

# Data dir (persistent: config.json, servers.json, cookies, logs)
ENV DATA_DIR=/data \
    PORT=80 \
    HOST=0.0.0.0
RUN mkdir -p /data
# Seed default config on first run via the app's _seed_data_dir(); provide a
# default config.json in the code dir so seeding has a source.
RUN cp -n config.example.json config.json

VOLUME ["/data"]
EXPOSE 80

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python3 -c "import os,urllib.request;urllib.request.urlopen('http://127.0.0.1:'+os.environ.get('PORT','80')+'/api/status',timeout=5).read()" || exit 1

CMD ["python3", "/app/server.py"]
