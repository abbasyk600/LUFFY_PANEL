FROM python:3.11-slim

WORKDIR /code

# Install system dependencies + Xray-core
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl unzip wget ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Xray-core
RUN ARCH=$(uname -m) && \
    case "$ARCH" in \
        x86_64)  XRAY_ARCH="linux-64" ;; \
        aarch64) XRAY_ARCH="linux-arm64-v8a" ;; \
        *)       XRAY_ARCH="linux-64" ;; \
    esac && \
    XRAY_VERSION="v25.5.21" && \
    wget -q "https://github.com/XTLS/Xray-core/releases/download/${XRAY_VERSION}/Xray-${XRAY_ARCH}.zip" -O /tmp/xray.zip && \
    unzip -q /tmp/xray.zip -d /tmp/xray && \
    mv /tmp/xray/xray /usr/local/bin/xray && \
    chmod +x /usr/local/bin/xray && \
    rm -rf /tmp/xray /tmp/xray.zip && \
    echo "Xray installed: $(xray version | head -1)"

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create data directories
RUN mkdir -p /data/hf /etc/xray

# Startup script
RUN echo '#!/bin/bash\n\
set -e\n\
echo "[Luffy] Starting Xray-core..."\n\
mkdir -p /data/hf/xray\n\
cat > /etc/xray/config.json << XRAYCONF\n\
{\n\
  "log": {"loglevel": "warning", "access": "/data/hf/xray/access.log", "error": "/data/hf/xray/error.log"},\n\
  "inbounds": [{\n\
    "port": 10000,\n\
    "listen": "127.0.0.1",\n\
    "protocol": "vless",\n\
    "settings": {\n\
      "clients": [{"id": "auto-generated-will-be-replaced"}],\n\
      "decryption": "none"\n\
    },\n\
    "streamSettings": {"network": "tcp"}\n\
  }],\n\
  "outbounds": [{\n\
    "protocol": "freedom",\n\
    "settings": {},\n\
    "tag": "direct"\n\
  }, {\n\
    "protocol": "blackhole",\n\
    "settings": {},\n\
    "tag": "block"\n\
  }],\n\
  "routing": {\n\
    "domainStrategy": "AsIs",\n\
    "rules": []\n\
  }\n\
}\n\
XRAYCONF\n\
echo "[Luffy] Xray config written"\n\
xray run -c /etc/xray/config.json &\n\
XRAY_PID=$!\n\
sleep 2\n\
if kill -0 $XRAY_PID 2>/dev/null; then\n\
    echo "[Luffy] Xray-core running (PID: $XRAY_PID) on port 10000"\n\
else\n\
    echo "[Luffy] WARNING: Xray-core may have failed to start"\n\
fi\n\
echo "[Luffy] Starting Luffy Panel on port 7860..."\n\
exec uvicorn main:app --host 0.0.0.0 --port 7860\n\
' > /start.sh && chmod +x /start.sh

# Hugging Face Spaces uses port 7860
EXPOSE 7860

CMD ["/start.sh"]
