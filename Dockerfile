FROM python:3.11-slim

RUN apt-get update && apt-get install -y fonts-dejavu-core && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY feed_processor.py .
COPY app.py .
COPY static/ static/

RUN mkdir -p data/images data/feeds data/results

ENV PORT=5000
ENV BASE_URL=http://localhost:5000/images/
ENV DATA_DIR=data

EXPOSE 5000
CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:5000", "--workers", "2", "--timeout", "120"]
