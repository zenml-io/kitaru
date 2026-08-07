# syntax=docker/dockerfile:1
# Build local candidate images from unpublished Kitaru and plugin wheels.

ARG PYTHON_VERSION=3.13
ARG UV_VERSION=0.12.1
ARG VIRTUAL_ENV=/app/.venv
ARG USERNAME=kitaru
ARG USER_UID=1000
ARG USER_GID=1000
ARG KITARU_VERSION=""

FROM docker.io/astral/uv:${UV_VERSION} AS uv

FROM python:${PYTHON_VERSION}-slim-bookworm AS base

ARG VIRTUAL_ENV
ARG USERNAME
ARG USER_UID
ARG USER_GID

RUN groupadd --gid $USER_GID $USERNAME && \
  useradd --uid $USER_UID --gid $USER_GID -m $USERNAME && \
  mkdir -p /app $VIRTUAL_ENV && \
  chown -R $USER_UID:$USER_GID /app

WORKDIR /app

FROM base AS builder

ARG VIRTUAL_ENV
ARG USERNAME
ARG USER_GID
ARG KITARU_VERSION
ARG KITARU_EXTRAS=""

COPY --from=uv /uv /uvx /bin/
COPY --chown=$USERNAME:$USER_GID pyproject.toml uv.lock ./
COPY --chown=$USERNAME:$USER_GID plugins/candidate-wheels ./candidate-wheels

ENV UV_COMPILE_BYTECODE=1 \
  UV_LINK_MODE=copy \
  UV_PYTHON_DOWNLOADS=never \
  UV_PROJECT_ENVIRONMENT=$VIRTUAL_ENV \
  VIRTUAL_ENV=$VIRTUAL_ENV \
  PATH="$VIRTUAL_ENV/bin:$PATH"

USER $USERNAME

RUN test -n "$KITARU_VERSION" && \
  test "$(uv version --short)" = "$KITARU_VERSION" && \
  uv sync --locked --no-dev --no-install-project $KITARU_EXTRAS && \
  uv pip install \
    --no-deps \
    --no-index \
    --find-links candidate-wheels \
    --only-binary=:all: \
    "kitaru==$KITARU_VERSION" && \
  uv pip check && \
  uv pip freeze > requirements.txt

FROM base AS runtime

ARG VIRTUAL_ENV
ARG USERNAME
ARG USER_GID

RUN --mount=from=uv,source=/uv,target=/bin/uv \
  uv pip uninstall --system pip setuptools wheel

COPY --chown=$USERNAME:$USER_GID \
  --from=builder $VIRTUAL_ENV $VIRTUAL_ENV
COPY --chown=$USERNAME:$USER_GID \
  --from=builder /app/requirements.txt /app/requirements.txt

ENV VIRTUAL_ENV=$VIRTUAL_ENV \
  PATH="$VIRTUAL_ENV/bin:/home/$USERNAME/.local/bin:$PATH" \
  PYTHONUNBUFFERED=1 \
  PYTHONFAULTHANDLER=1 \
  PYTHONHASHSEED=random

USER $USERNAME

FROM runtime AS client

FROM runtime AS server

EXPOSE 8000

CMD ["uvicorn", "kitaru.server.api.main:app", "--factory", "--no-server-header", "--proxy-headers", "--forwarded-allow-ips", "*", "--port", "8000", "--host", "0.0.0.0"]

FROM runtime AS worker

ARG USERNAME
ARG USER_GID

COPY --from=uv /uv /bin/uv
COPY --chown=$USERNAME:$USER_GID \
  --from=builder /app/candidate-wheels /app/candidate-wheels

ENV UV_FIND_LINKS=/app/candidate-wheels
