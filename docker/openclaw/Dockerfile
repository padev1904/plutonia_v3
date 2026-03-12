FROM node:22-slim

ARG OPENCLAW_NPM_PACKAGE=openclaw@2026.3.8

RUN apt-get update && apt-get install -y --no-install-recommends \
      bash \
      git ca-certificates \
      curl jq \
      procps \
      openssh-client \
      postgresql-client \
      netcat-openbsd \
      iputils-ping \
      rsync \
      python3 \
      python3-venv \
      python3-pip \
      ripgrep \
    && rm -rf /var/lib/apt/lists/*

COPY patch-openclaw-review-hardguard.mjs /tmp/patch-openclaw-review-hardguard.mjs
COPY init_openclaw_config.mjs /usr/local/bin/init_openclaw_config.mjs
COPY bootstrap_workspace_repo.sh /usr/local/bin/bootstrap_workspace_repo.sh
COPY seed_workspace.sh /usr/local/bin/seed_workspace.sh
COPY workspace/ /opt/openclaw-workspace/
COPY entrypoint.sh /usr/local/bin/openclaw-entrypoint.sh

RUN npm install -g "${OPENCLAW_NPM_PACKAGE}" \
    && node /tmp/patch-openclaw-review-hardguard.mjs \
    && rm -f /tmp/patch-openclaw-review-hardguard.mjs \
    && chmod +x /usr/local/bin/bootstrap_workspace_repo.sh \
    && chmod +x /usr/local/bin/seed_workspace.sh \
    && chmod +x /usr/local/bin/openclaw-entrypoint.sh

ENV HOME=/root
ENV OPENCLAW_CONFIG_DIR=/root/.openclaw

WORKDIR /root

ENTRYPOINT ["/usr/local/bin/openclaw-entrypoint.sh"]
CMD ["/usr/local/bin/openclaw", "gateway", "--port", "18789"]
