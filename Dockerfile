FROM python:3.11-slim

WORKDIR /code

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Hugging Face Spaces uses port 7860 by default
EXPOSE 7860

# Create data directory for persistent storage
RUN mkdir -p /data/hf

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7860"]
