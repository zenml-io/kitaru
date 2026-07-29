# syntax=docker/dockerfile:1

ARG PYTHON_IMAGE=python:3.13-slim-bookworm@sha256:9d7f287598e1a5a978c015ee176d8216435aaf335ed69ac3c38dd1bbb10e8d64
ARG UV_IMAGE=docker.io/astral/uv:0.8.3@sha256:ef11ed817e6a5385c02cd49fdcc99c23d02426088252a8eace6b6e6a2a511f36
ARG USERNAME=kitaru
ARG USER_UID=1000
ARG USER_GID=1000

FROM ${UV_IMAGE} AS uv

FROM ${PYTHON_IMAGE} AS base

ARG USERNAME
ARG USER_UID
ARG USER_GID

COPY --from=uv /uv /uvx /bin/

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

COPY dist/*.whl /tmp/wheels/
COPY dist/kitaru-server-requirements.txt /tmp/kitaru-server-requirements.txt

RUN uv venv /app/.venv && \
  wheel="$(find /tmp/wheels -name '*.whl' -print -quit)" && \
  test -n "$wheel" && \
  uv pip install --requirement /tmp/kitaru-server-requirements.txt && \
  uv pip install --no-deps "$wheel"

FROM base AS runtime

ARG USERNAME
ARG USER_GID

COPY --from=builder --chown=$USERNAME:$USER_GID /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

CMD ["python", "-m", "kitaru.server.api.main"]
