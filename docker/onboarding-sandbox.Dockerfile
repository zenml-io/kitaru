# syntax=docker/dockerfile:1
# Extend the published worker image with the tooling a coding agent needs, the
# Kitaru repository at the release tag, and the published Kitaru skills.

ARG BASE_IMAGE=zenmldocker/kitaru-worker:latest
ARG NODE_IMAGE=node:22-bookworm-slim
ARG SKILLS_VERSION=1.5.23
ARG USERNAME=kitaru
ARG USER_UID=1000
ARG USER_GID=1000

FROM ${NODE_IMAGE} AS node

FROM ${BASE_IMAGE} AS worker

ARG SKILLS_VERSION
ARG USERNAME
ARG USER_UID
ARG USER_GID

USER root

RUN apt-get update && \
  apt-get install -y --no-install-recommends ca-certificates curl git && \
  rm -rf /var/lib/apt/lists/*

COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/npm

RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm && \
  ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx && \
  node --version && npm --version && npx --version

# The published worker image installs only the worker extra.
RUN KITARU_VERSION="$(/app/.venv/bin/python -c 'import importlib.metadata as m; print(m.version("kitaru"))')" && \
  uv pip install --no-cache --python /app/.venv/bin/python \
    "kitaru[cli,mcp,worker]==$KITARU_VERSION" && \
  chown -R $USER_UID:$USER_GID /app/.venv

# Clone the repository at the release tag matching the installed package.
RUN KITARU_VERSION="$(/app/.venv/bin/python -c 'import importlib.metadata as m; print(m.version("kitaru"))')" && \
  git clone --depth 1 --branch "python/kitaru/v$KITARU_VERSION" \
    https://github.com/zenml-io/kitaru.git /opt/kitaru && \
  chown -R $USER_UID:$USER_GID /opt/kitaru

ENV HOME=/home/$USERNAME

USER $USERNAME

RUN npx --yes skills@$SKILLS_VERSION add zenml-io/kitaru-skills --global --all
