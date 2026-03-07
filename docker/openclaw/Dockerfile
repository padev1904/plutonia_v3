FROM node:22-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates \
      docker.io \
      docker-compose \
      curl jq \
    && rm -rf /var/lib/apt/lists/*

COPY patch-openclaw-review-hardguard.mjs /tmp/patch-openclaw-review-hardguard.mjs

RUN npm install -g openclaw@latest \
    && node /tmp/patch-openclaw-review-hardguard.mjs \
    && rm -f /tmp/patch-openclaw-review-hardguard.mjs

ENV HOME=/root
ENV OPENCLAW_CONFIG_DIR=/root/.openclaw

WORKDIR /root

CMD ["/usr/local/bin/openclaw", "gateway", "--port", "18789"]
