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

# Install Kitaru from local source so CI and release builds always test
# the checked-out code rather than a previously published PyPI version.
COPY . /tmp/kitaru
RUN cd /tmp/kitaru && pip install --no-cache-dir . && rm -rf /tmp/kitaru

# Replace the ZenML dashboard with the Kitaru UI.
# Downloads the released archive, verifies its checksum, and extracts
# into the installed ZenML package's dashboard directory.
RUN set -eux \
    && DASHBOARD_DIR="$(python -c 'import zenml; print(zenml.__path__[0])')/zen_server/dashboard" \
    && rm -rf "$DASHBOARD_DIR" \
    && mkdir -p "$DASHBOARD_DIR" \
    && curl -SsL "$KITARU_UI_REPO_URL/releases/download/$KITARU_UI_TAG/kitaru-ui.tar.gz" -o /tmp/kitaru-ui.tar.gz \
    && curl -SsL "$KITARU_UI_REPO_URL/releases/download/$KITARU_UI_TAG/kitaru-ui.tar.gz.sha256" -o /tmp/kitaru-ui.tar.gz.sha256 \
    && cd /tmp && sha256sum -c kitaru-ui.tar.gz.sha256 \
    && tar xzf /tmp/kitaru-ui.tar.gz -C "$DASHBOARD_DIR" \
    && rm -f /tmp/kitaru-ui.tar.gz /tmp/kitaru-ui.tar.gz.sha256 \
    && test -f "$DASHBOARD_DIR/index.html" \
    || { echo "Kitaru UI assets missing: $DASHBOARD_DIR/index.html" >&2; exit 1; }

ENV KITARU_DEBUG=false \
    KITARU_ANALYTICS_OPT_IN=true

EXPOSE 8080
