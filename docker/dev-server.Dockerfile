# syntax=docker/dockerfile:1
# Build local and production-style server development images from locked source.

ARG PYTHON_VERSION=3.13
ARG UV_VERSION=0.12.1
ARG VIRTUAL_ENV=/app/.venv
ARG USERNAME=kitaru
ARG USER_UID=1000
ARG USER_GID=1000
ARG INSTALL_DEBUG_TOOLS=false

FROM docker.io/astral/uv:${UV_VERSION} AS uv

ARG PYTHON_VERSION
ARG UV_VERSION
ARG VIRTUAL_ENV
ARG USERNAME
ARG USER_UID
ARG USER_GID
ARG INSTALL_DEBUG_TOOLS

FROM python:${PYTHON_VERSION}-slim-bookworm AS base

ARG PYTHON_VERSION
ARG UV_VERSION
ARG VIRTUAL_ENV
ARG USERNAME
ARG USER_UID
ARG USER_GID
ARG INSTALL_DEBUG_TOOLS

# Keep runtime utilities opt-in. PostgreSQL is Kitaru's supported database, so
# its client replaces the MySQL and MariaDB clients used by the ZenML image.
RUN set -ex && \
  apt-get update && \
  apt-get upgrade -y && \
  if [ "$INSTALL_DEBUG_TOOLS" = "true" ]; then \
    apt-get install -y --no-install-recommends \
      curl \
      git \
      inetutils-ping \
      net-tools \
      nmap \
      postgresql-client; \
  fi && \
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

FROM base AS pre-builder

ARG PYTHON_VERSION
ARG UV_VERSION
ARG VIRTUAL_ENV
ARG USERNAME
ARG USER_UID
ARG USER_GID
ARG INSTALL_DEBUG_TOOLS

COPY --from=uv /uv /uvx /bin/
COPY --chown=$USERNAME:$USER_GID pyproject.toml uv.lock ./

ENV UV_COMPILE_BYTECODE=1 \
  UV_LINK_MODE=copy \
  UV_PYTHON_DOWNLOADS=never \
  UV_PROJECT_ENVIRONMENT=$VIRTUAL_ENV \
  VIRTUAL_ENV=$VIRTUAL_ENV \
  PATH="$VIRTUAL_ENV/bin:$PATH"

USER $USERNAME

# Cache the complete locked server dependency environment independently from
# source changes. The project itself is installed in the target-specific stage.
RUN uv sync \
  --locked \
  --no-dev \
  --no-install-project \
  --extra server \
  --extra s3 \
  --extra otel

FROM pre-builder AS common-runtime

ARG PYTHON_VERSION
ARG UV_VERSION
ARG VIRTUAL_ENV
ARG USERNAME
ARG USER_UID
ARG USER_GID
ARG INSTALL_DEBUG_TOOLS

COPY --chown=$USERNAME:$USER_GID README.md ./
COPY --chown=$USERNAME:$USER_GID src ./src

ENV PYTHONUNBUFFERED=1 \
  PYTHONFAULTHANDLER=1 \
  PYTHONHASHSEED=random \
  KITARU_SERVER_ANALYTICS_DEBUG=true \
  KITARU_SERVER_LOG_LEVEL=DEBUG

# Keep the project editable so a source bind mount is immediately visible to
# the reload process while retaining installed package metadata.
RUN uv sync --locked --no-dev --extra server --extra s3 --extra otel && \
  uv pip check && \
  python -c \
    "from kitaru.server.api.main import app; assert callable(app)" && \
  uv pip freeze > requirements.txt

FROM common-runtime AS local-runtime

ARG PYTHON_VERSION
ARG UV_VERSION
ARG VIRTUAL_ENV
ARG USERNAME
ARG USER_UID
ARG USER_GID
ARG INSTALL_DEBUG_TOOLS

EXPOSE 8000

CMD ["uvicorn", "kitaru.server.api.main:app", "--factory", "--log-level", "debug", "--no-server-header", "--proxy-headers", "--forwarded-allow-ips", "*", "--reload", "--port", "8000", "--host", "0.0.0.0"]

FROM pre-builder AS builder

ARG PYTHON_VERSION
ARG UV_VERSION
ARG VIRTUAL_ENV
ARG USERNAME
ARG USER_UID
ARG USER_GID
ARG INSTALL_DEBUG_TOOLS

COPY --chown=$USERNAME:$USER_GID README.md ./
COPY --chown=$USERNAME:$USER_GID src ./src

# Install the repository project non-editably for the production-style image.
RUN uv sync \
  --locked \
  --no-dev \
  --no-editable \
  --extra server \
  --extra s3 \
  --extra otel && \
  uv pip check && \
  python -c \
    "from kitaru.server.api.main import app; assert callable(app)" && \
  uv pip freeze > requirements.txt

FROM base AS runtime

ARG PYTHON_VERSION
ARG UV_VERSION
ARG VIRTUAL_ENV
ARG USERNAME
ARG USER_UID
ARG USER_GID
ARG INSTALL_DEBUG_TOOLS

COPY --chown=$USERNAME:$USER_GID \
  --from=builder $VIRTUAL_ENV $VIRTUAL_ENV
COPY --chown=$USERNAME:$USER_GID \
  --from=builder /app/requirements.txt /app/requirements.txt

ENV VIRTUAL_ENV=$VIRTUAL_ENV \
  PATH="$VIRTUAL_ENV/bin:/home/$USERNAME/.local/bin:$PATH" \
  PYTHONUNBUFFERED=1 \
  PYTHONFAULTHANDLER=1 \
  PYTHONHASHSEED=random \
  KITARU_SERVER_ANALYTICS_DEBUG=true \
  KITARU_SERVER_LOG_LEVEL=DEBUG

USER $USERNAME

EXPOSE 8000

CMD ["uvicorn", "kitaru.server.api.main:app", "--factory", "--log-level", "debug", "--no-server-header", "--proxy-headers", "--forwarded-allow-ips", "*", "--port", "8000", "--host", "0.0.0.0"]
