FROM python:3.11-slim

WORKDIR /code

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl unzip wget ca-certificates procps \
    && rm -rf /var/lib/apt/lists/*

# Install Xray-core (latest stable)
RUN ARCH=$(uname -m) && \
    case "$ARCH" in \
        x86_64)  XRAY_ARCH="linux-64" ;; \
        aarch64) XRAY_ARCH="linux-arm64-v8a" ;; \
        *)       XRAY_ARCH="linux-64" ;; \
    esac && \
    XRAY_VER="v26.3.27" && \
    wget -q --retry-connrefused --tries=5 --timeout=30 \
      "https://github.com/XTLS/Xray-core/releases/download/${XRAY_VER}/Xray-${XRAY_ARCH}.zip" \
      -O /tmp/xray.zip && \
    unzip -q /tmp/xray.zip -d /tmp/xray && \
    mv /tmp/xray/xray /usr/local/bin/xray && \
    chmod +x /usr/local/bin/xray && \
    rm -rf /tmp/xray /tmp/xray.zip && \
    echo "Xray installed: $(/usr/local/bin/xray version | head -1)"

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY . .

# Create data dirs and startup script
RUN mkdir -p /data/hf /etc/xray

RUN printf '#!/bin/bash\n\
set -e\n\
echo "[Luffy] Starting Xray-core on port 10000..."\n\
mkdir -p /data/hf/xray\n\
xray run -c /etc/xray/config.json &\n\
XRAY_PID=$!\n\
sleep 2\n\
if kill -0 $XRAY_PID 2>/dev/null; then\n\
    echo "[Luffy] Xray PID=$XRAY_PID running"\n\
else\n\
    echo "[Luffy] WARNING: Xray failed to start"\n\
fi\n\
echo "[Luffy] Starting panel on :7860"\n\
exec uvicorn main:app --host 0.0.0.0 --port 7860\n\
' > /start.sh && chmod +x /start.sh

EXPOSE 7860
CMD ["/start.sh"]
