# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.13
ARG USERNAME=kitaru
ARG USER_UID=1000
ARG USER_GID=1000

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
  PATH="/home/$USERNAME/.local/bin:$PATH"

FROM base AS builder

ARG USERNAME
ARG USER_GID

COPY --chown=$USERNAME:$USER_GID pyproject.toml uv.lock README.md LICENSE ./
COPY --chown=$USERNAME:$USER_GID src ./src
COPY --chown=$USERNAME:$USER_GID plugins ./plugins

RUN uv sync --locked --no-dev --extra server --extra importers --no-editable

FROM base AS runtime

ARG USERNAME
ARG USER_GID

COPY --from=builder --chown=$USERNAME:$USER_GID /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

CMD ["python", "-m", "kitaru.server.api.main"]
