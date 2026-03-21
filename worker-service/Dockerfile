FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libfreetype6-dev \
    libjpeg-dev \
    libpng-dev \
    fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY feed_processor.py .
COPY worker.py .

ENV PORT=8080
ENV PYTHONUNBUFFERED=1

CMD exec gunicorn worker:app \
    --bind 0.0.0.0:$PORT \
    --workers 1 \
    --threads 2 \
    --timeout 3600
