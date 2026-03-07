FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY portal/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY portal/ /app/
COPY scripts/ /opt/ai-news-scripts/

RUN chmod +x /opt/ai-news-scripts/*.py /opt/ai-news-scripts/*.sh 2>/dev/null || true

EXPOSE 8000

CMD ["gunicorn", "config.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "4", "--timeout", "120"]
