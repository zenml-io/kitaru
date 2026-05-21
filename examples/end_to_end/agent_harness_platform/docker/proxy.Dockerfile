# DockerProxy image — mitmdump + the credential-injection addon.
# Build context: the example root (so the COPY can reach the adapter).
#   docker build -t agent-harness-platform-proxy -f docker/proxy.Dockerfile .
FROM mitmproxy/mitmproxy:latest

# Bake in the addon. Cert files are mounted at runtime (per host).
COPY agent_harness_platform/sandbox/proxy_addon.py /opt/proxy_addon.py

# `nc` for the readiness probe in DockerProxy._wait_until_ready
RUN pip install --break-system-packages --no-cache-dir mitmproxy 2>/dev/null || true \
    && apt-get update \
    && apt-get install -y --no-install-recommends netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

CMD ["mitmdump", \
     "--quiet", \
     "--listen-host", "0.0.0.0", \
     "--listen-port", "8080", \
     "--set", "confdir=/certs", \
     "-s", "/opt/proxy_addon.py"]
