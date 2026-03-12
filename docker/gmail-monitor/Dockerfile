FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    git \
    ca-certificates \
    gnupg \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
        | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_22.x nodistro main" \
        > /etc/apt/sources.list.d/nodesource.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends nodejs \
    && npm install -g npm@latest \
    && rm -rf /var/lib/apt/lists/*

COPY portal/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
RUN npm install -g openclaw@latest

COPY portal/ /app/
COPY scripts/ /app/scripts/

ENV PYTHONPATH=/app:/app/scripts

RUN chmod +x /app/scripts/*.py /app/scripts/*.sh 2>/dev/null || true

EXPOSE 8001

CMD ["python", "/app/scripts/gmail_monitor_supervisor.py", "--", "python", "/app/scripts/gmail_monitor.py", "--mode", "monitor"]
