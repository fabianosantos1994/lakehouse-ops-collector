# Imagem do lakehouse-ops-collector: poller e API de detecção (fase 3).
# Construída e executada pela stack `ops` do lakehouse-infra; cada serviço
# escolhe o comando. A configuração vem só de variáveis de ambiente.
FROM python:3.12.11-slim-bullseye

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install . && useradd --create-home --uid 10001 ops

USER ops
CMD ["python", "-m", "lakehouse_ops_collector.detection.poller"]
