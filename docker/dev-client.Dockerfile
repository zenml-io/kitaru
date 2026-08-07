# syntax=docker/dockerfile:1
# Build the client development image from the locked repository source.

ARG PYTHON_VERSION=3.13
ARG UV_VERSION=0.12.1
ARG VIRTUAL_ENV=/app/.venv
ARG USERNAME=kitaru
ARG USER_UID=1000
ARG USER_GID=1000

FROM docker.io/astral/uv:${UV_VERSION} AS uv

ARG PYTHON_VERSION
ARG UV_VERSION
ARG VIRTUAL_ENV
ARG USERNAME
ARG USER_UID
ARG USER_GID

FROM python:${PYTHON_VERSION}-slim-bookworm AS base

ARG PYTHON_VERSION
ARG UV_VERSION
ARG VIRTUAL_ENV
ARG USERNAME
ARG USER_UID
ARG USER_GID

RUN set -ex && \
  apt-get update && \
  apt-get upgrade -y && \
  apt-get autoremove -y && \
  apt-get clean -y && \
  rm -rf /var/lib/apt/lists/* && \
  groupadd --gid $USER_GID $USERNAME && \
  useradd --uid $USER_UID --gid $USER_GID -m $USERNAME && \
  mkdir -p /app $VIRTUAL_ENV && \
  chown -R $USER_UID:$USER_GID /app

RUN --mount=from=uv,source=/uv,target=/bin/uv \
  uv pip uninstall --system pip setuptools wheel

WORKDIR /app

FROM base AS builder

ARG PYTHON_VERSION
ARG UV_VERSION
ARG VIRTUAL_ENV
ARG USERNAME
ARG USER_UID
ARG USER_GID

COPY --from=uv /uv /uvx /bin/
COPY --chown=$USERNAME:$USER_GID pyproject.toml uv.lock README.md ./
COPY --chown=$USERNAME:$USER_GID plugins/packages ./plugins/packages
COPY --chown=$USERNAME:$USER_GID src ./src

ENV UV_COMPILE_BYTECODE=1 \
  UV_LINK_MODE=copy \
  UV_PYTHON_DOWNLOADS=never \
  UV_PROJECT_ENVIRONMENT=$VIRTUAL_ENV \
  VIRTUAL_ENV=$VIRTUAL_ENV \
  PATH="$VIRTUAL_ENV/bin:$PATH"

USER $USERNAME

# Install the repository project and its client dependencies from the lockfile.
# Keep a snapshot of the resulting environment for external inspection.
RUN uv sync --locked --package kitaru --no-dev --no-editable && \
  uv pip check && \
  python -c "import kitaru" && \
  uv pip freeze > requirements.txt

FROM base AS client

ARG PYTHON_VERSION
ARG UV_VERSION
ARG VIRTUAL_ENV
ARG USERNAME
ARG USER_UID
ARG USER_GID

COPY --chown=$USERNAME:$USER_GID \
  --from=builder $VIRTUAL_ENV $VIRTUAL_ENV
COPY --chown=$USERNAME:$USER_GID \
  --from=builder /app/requirements.txt /app/requirements.txt

ENV VIRTUAL_ENV=$VIRTUAL_ENV \
  PATH="$VIRTUAL_ENV/bin:/home/$USERNAME/.local/bin:$PATH" \
  PYTHONUNBUFFERED=1 \
  PYTHONFAULTHANDLER=1 \
  PYTHONHASHSEED=random \
  KITARU_SERVER_ANALYTICS_DEBUG=true

USER $USERNAME
