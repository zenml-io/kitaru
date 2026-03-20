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
ARG ZENML_REPO_URL=https://github.com/zenml-io/zenml.git
ARG ZENML_GIT_REF=a14c7d4dd43b5170617cc865395fb03589ba062a
ARG ZENML_DASHBOARD_TAG=v0.41.4
ARG ZENML_SERVER_EXTRAS=server,secrets-aws,secrets-gcp,secrets-azure,secrets-hashicorp,s3fs,gcsfs,adlfs,connectors-aws,connectors-gcp,connectors-azure,azureml,sagemaker,vertex

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
ARG ZENML_REPO_URL
ARG ZENML_GIT_REF
ARG ZENML_DASHBOARD_TAG
ARG ZENML_SERVER_EXTRAS

ENV \
  VIRTUAL_ENV=$VIRTUAL_ENV \
  PIP_DEFAULT_TIMEOUT=100 \
  PIP_DISABLE_PIP_VERSION_CHECK=1 \
  PIP_NO_CACHE_DIR=1

WORKDIR /build

# The temporary ZenML git-ref packaging flow needs git for checkout and the
# dashboard bundling script needs curl/openssl/tar/CA certs.
RUN apt-get update && apt-get install -y --no-install-recommends \
      bash \
      ca-certificates \
      curl \
      git \
      openssl \
      tar \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN pip install --upgrade pip uv setuptools

# Install Kitaru from local source first. The authoritative ZenML install comes
# afterwards because Kitaru's dependency metadata still points at the raw git
# URL and would otherwise overwrite the dashboard-bundled install.
COPY . /tmp/kitaru
RUN cd /tmp/kitaru && uv pip install . && rm -rf /tmp/kitaru

# Install ZenML with the same server + cloud extras that the official
# zenmldocker/zenml-server image ships. Alembic is pinned for migration
# stability (matches ZenML's own pin).
#
# TODO: Switch to released PyPI version once the zenml release is out:
#   uv pip install "zenml[server,secrets-aws,secrets-gcp,secrets-azure,
#     secrets-hashicorp,s3fs,gcsfs,adlfs,connectors-aws,connectors-gcp,
#     connectors-azure,azureml,sagemaker,vertex]>=X.Y.Z" "alembic==1.15.2"
# Pinned to commit hash for tester stability; bump hash when updating.
RUN git clone "$ZENML_REPO_URL" /build/zenml \
    && cd /build/zenml \
    && git checkout "$ZENML_GIT_REF" \
    && TAG="$ZENML_DASHBOARD_TAG" bash scripts/install-dashboard.sh \
    && uv pip install --reinstall ".[${ZENML_SERVER_EXTRAS}]" "alembic==1.15.2"

# Fail the build early if the installed ZenML package still lacks the dashboard
# bundle required by browser-based device authentication.
RUN DASHBOARD="$(python -c 'import zenml; print(zenml.__path__[0])')/zen_server/dashboard/index.html" \
    && test -f "$DASHBOARD" || { echo "Dashboard assets missing: $DASHBOARD" >&2; exit 1; } \
    && echo "Verified ZenML dashboard assets at $DASHBOARD"

RUN pip freeze > requirements.txt \
    && sed -i "s|zenml @ file:///build/zenml|zenml @ git+${ZENML_REPO_URL}@${ZENML_GIT_REF}|" requirements.txt

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
  KITARU_ANALYTICS_OPT_IN=true \
  # User-facing analytics source knob (translated to ZENML_* by kitaru.__init__
  # in client processes). The server process runs zenml.zen_server directly
  # without ever importing kitaru, so infer_source_context() would never see
  # the translation. ZENML_DEFAULT_ANALYTICS_SOURCE is set here directly so
  # ZenML middleware classifies header-less requests (dashboard, internal calls)
  # as "kitaru-api" rather than the plain "api" fallback.
  KITARU_DEFAULT_ANALYTICS_SOURCE=kitaru-api \
  ZENML_DEFAULT_ANALYTICS_SOURCE=kitaru-api

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
