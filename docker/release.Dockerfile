# syntax=docker/dockerfile:1
# Build official release images from the published PyPI package.

ARG PYTHON_VERSION=3.13
ARG USERNAME=kitaru
ARG USER_UID=1000
ARG USER_GID=1000
ARG KITARU_VERSION=""

FROM python:${PYTHON_VERSION}-slim-bookworm AS base

ARG USERNAME
ARG USER_UID
ARG USER_GID

RUN set -ex && \
  apt-get update && \
  apt-get upgrade -y && \
  apt-get install -y --no-install-recommends curl && \
  apt-get autoremove -y && \
  apt-get clean -y && \
  rm -rf /var/lib/apt/lists/*

COPY --from=docker.io/astral/uv:latest /uv /uvx /bin/

RUN groupadd --gid $USER_GID $USERNAME && \
  useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

USER $USERNAME
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
  UV_LINK_MODE=copy \
  UV_PYTHON_DOWNLOADS=never \
  VIRTUAL_ENV=/app/.venv \
  PATH="/home/$USERNAME/.local/bin:$PATH"

FROM base AS builder

ARG KITARU_VERSION

RUN uv venv /app/.venv && \
  uv pip install "kitaru[server,otel]${KITARU_VERSION:+==$KITARU_VERSION}"

FROM base AS runtime

ARG USERNAME
ARG USER_GID

COPY --from=builder --chown=$USERNAME:$USER_GID /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

CMD ["python", "-m", "kitaru.server.api.main"]
