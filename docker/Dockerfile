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

ARG ZENML_SERVER_TAG=0.94.1

FROM zenmldocker/zenml-server:${ZENML_SERVER_TAG} AS server

ARG KITARU_UI_TAG=latest
ARG KITARU_UI_REPO_URL=https://github.com/zenml-io/kitaru-ui

# The base image runs as non-root user "zenml" (UID 1000).
# Switch to root for package installation and file operations.
USER root

# Install curl for downloading release assets (not in the slim base image).
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Install Kitaru from local source so CI and release builds always test
# the checked-out code rather than a previously published PyPI version.
COPY . /tmp/kitaru
RUN cd /tmp/kitaru && pip install --no-cache-dir . && rm -rf /tmp/kitaru

# Replace the ZenML dashboard with the Kitaru UI.
# Downloads the released archive, verifies its checksum, and extracts
# into the installed ZenML package's dashboard directory.
# When KITARU_UI_TAG is "latest", uses GitHub's latest-release URL shortcut.
RUN set -eux \
    && DASHBOARD_DIR="$(python -c 'import zenml; print(zenml.__path__[0])')/zen_server/dashboard" \
    && rm -rf "$DASHBOARD_DIR" \
    && mkdir -p "$DASHBOARD_DIR" \
    && if [ "$KITARU_UI_TAG" = "latest" ]; then \
         BASE_URL="$KITARU_UI_REPO_URL/releases/latest/download"; \
       else \
         BASE_URL="$KITARU_UI_REPO_URL/releases/download/$KITARU_UI_TAG"; \
       fi \
    && curl -fSsL "$BASE_URL/kitaru-ui.tar.gz" -o /tmp/kitaru-ui.tar.gz \
    && curl -fSsL "$BASE_URL/kitaru-ui.tar.gz.sha256" -o /tmp/kitaru-ui.tar.gz.sha256 \
    && cd /tmp && sha256sum -c kitaru-ui.tar.gz.sha256 \
    && tar xzf /tmp/kitaru-ui.tar.gz -C "$DASHBOARD_DIR" \
    && rm -f /tmp/kitaru-ui.tar.gz /tmp/kitaru-ui.tar.gz.sha256

# Verify the dashboard sentinel file exists (separate RUN so download/checksum
# failures above surface their own error messages via set -e).
RUN test -f "$(python -c 'import zenml; print(zenml.__path__[0])')/zen_server/dashboard/index.html" \
    || { echo "Kitaru UI assets missing: index.html not found in dashboard directory" >&2; exit 1; }

# Switch back to non-root user (matches base image).
USER zenml

ENV KITARU_DEBUG=false \
    KITARU_ANALYTICS_OPT_IN=true

EXPOSE 8080
