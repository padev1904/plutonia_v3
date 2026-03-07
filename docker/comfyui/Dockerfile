FROM nvidia/cuda:12.8.0-runtime-ubuntu24.04

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-pip python3-venv \
      git curl wget ca-certificates \
      libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth=1 --retry 3 https://github.com/comfyanonymous/ComfyUI.git /comfyui || \
    (sleep 10 && git clone --depth=1 https://github.com/comfyanonymous/ComfyUI.git /comfyui)

WORKDIR /comfyui

RUN python3 -m venv .venv && \
    .venv/bin/pip install --upgrade pip && \
    .venv/bin/pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu128 && \
    .venv/bin/pip install -r requirements.txt

RUN mkdir -p models output

ENV PATH="/comfyui/.venv/bin:$PATH"

EXPOSE 8188

CMD ["python3", "main.py", "--listen", "0.0.0.0", "--port", "8188"]
