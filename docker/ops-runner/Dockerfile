FROM docker:27-cli

RUN apk add --no-cache \
      bash \
      curl \
      git \
      jq \
      python3 \
      py3-pip

WORKDIR /app

COPY scripts/ops_runner.py /app/ops_runner.py

ENV PYTHONUNBUFFERED=1

CMD ["python3", "/app/ops_runner.py"]
