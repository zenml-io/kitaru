# Production Kitaru server image.
#
# Layers Kitaru on top of the official ZenML server image, then replaces
# the bundled ZenML dashboard with the Kitaru UI.
#
# This is NOT the flow-execution image (see Dockerfile.dev).
#
# Build:
#   just server-image
#
# Run:
#   docker run -p 8080:8080 zenmldocker/kitaru:latest
#
# Published to: zenmldocker/kitaru

ARG ZENML_SERVER_TAG=0.96.1

FROM zenmldocker/zenml-server:${ZENML_SERVER_TAG} AS server

ARG KITARU_VERSION=""

# The base image runs as non-root user "zenml" (UID 1000).
# Switch to root for package installation and file operations.
USER root

# Install uv (fast Python package installer). uv is copied as a static
# binary from the distroless image.
COPY --from=ghcr.io/astral-sh/uv:0.10 /uv /uvx /bin/

# Install Kitaru. When KITARU_VERSION is set (release builds), install from
# PyPI for provenance. Otherwise install from the checked-out source tree
# (CI and local builds).
COPY . /tmp/kitaru
RUN if [ -n "$KITARU_VERSION" ]; then \
      uv pip install --no-cache "kitaru==$KITARU_VERSION"; \
    else \
      uv pip install --no-cache /tmp/kitaru; \
    fi \
    && rm -rf /tmp/kitaru

# Replace the ZenML dashboard with the Kitaru UI already bundled inside the
# installed Kitaru package. The release workflow is responsible for downloading
# the stable UI before building the wheel; Docker must not download a second UI.
RUN set -eux \
    && KITARU_UI_DIST="$(python -c 'from pathlib import Path; import kitaru; print(Path(kitaru.__file__).parent / "_ui" / "dist")')" \
    && test -f "$KITARU_UI_DIST/index.html" \
       || { echo "Kitaru package UI assets missing: index.html not found in $KITARU_UI_DIST" >&2; exit 1; } \
    && DASHBOARD_DIR="$(python -c 'import zenml; print(zenml.__path__[0])')/zen_server/dashboard" \
    && rm -rf "$DASHBOARD_DIR" \
    && mkdir -p "$DASHBOARD_DIR" \
    && cp -a "$KITARU_UI_DIST/." "$DASHBOARD_DIR/" \
    && { test -f "$DASHBOARD_DIR/index.html" \
         || { echo "Kitaru UI assets missing: index.html not found in dashboard directory" >&2; exit 1; }; }

# Switch back to non-root user (matches base image).
USER zenml

# Keep workload-manager support enabled so snapshot-backed deployment
# invocation paths (`kitaru invoke` / deployment curl) run on the server.
ENV KITARU_DEBUG=false \
    KITARU_ANALYTICS_OPT_IN=true \
    KITARU_DEFAULT_ANALYTICS_SOURCE=kitaru-api \
    ZENML_DEFAULT_ANALYTICS_SOURCE=kitaru-api \
    ZENML_SERVER_WORKLOAD_MANAGER_IMPLEMENTATION_SOURCE=zenml.zen_server.pipeline_execution.in_memory_workload_manager.InMemoryWorkloadManager

EXPOSE 8080
