# Production Kitaru server image.
#
# Builds a ZenML server with all cloud plugins, then layers Kitaru on top.
# The resulting image is the user-facing "Kitaru server" — it runs the ZenML
# API server with Kitaru installed and (eventually) the Kitaru dashboard
# bundled in.
#
# This is NOT the flow-execution image (see Dockerfile.dev and ImageSettings).
#
# Build:
#   just server-image
#
# Run:
#   docker run -p 8080:8080 zenmldocker/kitaru:latest
#
# Published to: zenmldocker/kitaru

ARG PYTHON_VERSION=3.11
ARG VIRTUAL_ENV=/opt/venv
ARG USERNAME=kitaru
ARG USER_UID=1000
ARG USER_GID=1000

# ---------------------------------------------------------------------------
# Base: minimal Python runtime
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim-bookworm AS base

RUN set -ex \
  && apt-get update \
  && apt-get upgrade -y \
  && apt-get autoremove -y \
  && apt-get clean -y \
  && rm -rf /var/lib/apt/lists/* \
  && pip install --upgrade pip setuptools

# ---------------------------------------------------------------------------
# Builder: install ZenML server + cloud extras + Kitaru into a venv
# ---------------------------------------------------------------------------
FROM base AS builder

ARG VIRTUAL_ENV

ENV \
  VIRTUAL_ENV=$VIRTUAL_ENV \
  PIP_DEFAULT_TIMEOUT=100 \
  PIP_DISABLE_PIP_VERSION_CHECK=1 \
  PIP_NO_CACHE_DIR=1

WORKDIR /build

# git is needed for the branch-based ZenML dependency (temporary)
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install ZenML with the same server + cloud extras that the official
# zenmldocker/zenml-server image ships.  Alembic is pinned for migration
# stability (matches ZenML's own pin).
#
# TODO: Switch to released PyPI version once feature/kitaru is merged:
#   uv pip install "zenml[server,secrets-aws,secrets-gcp,secrets-azure,
#     secrets-hashicorp,s3fs,gcsfs,adlfs,connectors-aws,connectors-gcp,
#     connectors-azure,azureml,sagemaker,vertex]>=X.Y.Z" "alembic==1.15.2"
# Pinned to commit hash for tester stability; bump hash when updating
RUN pip install --upgrade pip uv setuptools \
    && uv pip install \
      "zenml[server,secrets-aws,secrets-gcp,secrets-azure,secrets-hashicorp,s3fs,gcsfs,adlfs,connectors-aws,connectors-gcp,connectors-azure,azureml,sagemaker,vertex] @ git+https://github.com/zenml-io/zenml.git@a2d00d5cc3cc7dce2b2aeef17bd04e00fad067fb" \
      "alembic==1.15.2"

# Install Kitaru from local source
COPY . /tmp/kitaru
RUN cd /tmp/kitaru && uv pip install . && rm -rf /tmp/kitaru

RUN pip freeze > requirements.txt

# ---------------------------------------------------------------------------
# Server: clean runtime image
# ---------------------------------------------------------------------------
FROM base AS server

ARG VIRTUAL_ENV
ARG USERNAME
ARG USER_UID
ARG USER_GID

ENV \
  PYTHONUNBUFFERED=1 \
  PYTHONFAULTHANDLER=1 \
  PYTHONHASHSEED=random \
  VIRTUAL_ENV=$VIRTUAL_ENV \
  # Signal to ZenML that it is running in a container
  ZENML_CONTAINER=1 \
  # ZenML server configuration paths
  ZENML_CONFIG_PATH=/zenml/.zenconfig \
  KITARU_DEBUG=false \
  KITARU_ANALYTICS_OPT_IN=true

# Set these to configure the server connection without `kitaru login`:
# KITARU_SERVER_URL=https://...
# KITARU_AUTH_TOKEN=...
# KITARU_PROJECT=...

WORKDIR /zenml

# Create non-root user and pre-create local stores directory
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && mkdir -p /zenml/.zenconfig/local_stores/default_zen_store \
    && chown -R $USER_UID:$USER_GID /zenml

# Copy the virtual environment from the builder stage
COPY --chown=$USERNAME:$USER_GID --from=builder /opt/venv /opt/venv
# Copy the requirements.txt for reproducibility auditing
COPY --chown=$USERNAME:$USER_GID --from=builder /build/requirements.txt /zenml/requirements.txt
# Copy the entrypoint script
COPY --chown=$USERNAME:$USER_GID docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# TODO: Bundle kitaru-ui dashboard assets here once available.
# The Kitaru UI will be served on the same port as the API server.
# Expected approach: assets are bundled into the kitaru Python package
# (so PyPI and Docker use the same source), then served by the ZenML
# server at a configured static path.

ENV PATH="$VIRTUAL_ENV/bin:/home/$USERNAME/.local/bin:$PATH"

# Switch to non-privileged user
USER $USERNAME

EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
CMD ["uvicorn", "zenml.zen_server.zen_server_api:app", \
     "--no-server-header", "--proxy-headers", \
     "--forwarded-allow-ips", "*", \
     "--port", "8080", "--host", "0.0.0.0"]
