# syntax=docker/dockerfile:1
# Build the client release image from the published PyPI package and the
# dependency versions locked by the matching repository release.

ARG PYTHON_VERSION=3.13
ARG UV_VERSION=0.12.1
ARG VIRTUAL_ENV=/app/.venv
ARG USERNAME=kitaru
ARG USER_UID=1000
ARG USER_GID=1000
ARG KITARU_VERSION=""

FROM docker.io/astral/uv:${UV_VERSION} AS uv

ARG PYTHON_VERSION
ARG UV_VERSION
ARG VIRTUAL_ENV
ARG USERNAME
ARG USER_UID
ARG USER_GID
ARG KITARU_VERSION

FROM python:${PYTHON_VERSION}-slim-bookworm AS base

ARG PYTHON_VERSION
ARG UV_VERSION
ARG VIRTUAL_ENV
ARG USERNAME
ARG USER_UID
ARG USER_GID
ARG KITARU_VERSION

RUN groupadd --gid $USER_GID $USERNAME && \
  useradd --uid $USER_UID --gid $USER_GID -m $USERNAME && \
  mkdir -p /app $VIRTUAL_ENV && \
  chown -R $USER_UID:$USER_GID /app

WORKDIR /app

FROM base AS builder

ARG PYTHON_VERSION
ARG UV_VERSION
ARG VIRTUAL_ENV
ARG USERNAME
ARG USER_UID
ARG USER_GID
ARG KITARU_VERSION

COPY --from=uv /uv /uvx /bin/
COPY --chown=$USERNAME:$USER_GID pyproject.toml uv.lock ./
COPY --chown=$USERNAME:$USER_GID \
  plugins/pyproject.toml plugins/README.md ./plugins/

ENV UV_COMPILE_BYTECODE=1 \
  UV_LINK_MODE=copy \
  UV_PYTHON_DOWNLOADS=never \
  UV_PROJECT_ENVIRONMENT=$VIRTUAL_ENV \
  VIRTUAL_ENV=$VIRTUAL_ENV \
  PATH="$VIRTUAL_ENV/bin:$PATH"

USER $USERNAME

# Install the client dependencies from the lockfile, then install the matching
# published Kitaru wheel without resolving its dependencies again. Keep a
# snapshot of the resulting environment for inspection outside the container.
RUN test -n "$KITARU_VERSION" && \
  test "$(uv version --short)" = "$KITARU_VERSION" && \
  uv sync --locked --no-dev --no-install-workspace && \
  uv pip install \
    --no-deps \
    --only-binary=:all: \
    "kitaru==$KITARU_VERSION" && \
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
ARG KITARU_VERSION

# The Python base image includes package-management tools that are unnecessary
# at runtime. uv is mounted only for this command and is not included in the
# resulting image.
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
