FROM python:3.11-slim

RUN apt-get update && apt-get install -y fonts-dejavu-core && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY feed_processor.py .
COPY app.py .
COPY static/ static/

RUN mkdir -p data/images data/feeds data/results

ENV PORT=8080
ENV BASE_URL=http://localhost:8080/images/
ENV DATA_DIR=data

EXPOSE 8080
CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "120"]
