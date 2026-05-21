# Worker image for agent_harness_platform. Minimal — bash + curl + jq.
# Stage 3 adds a CA cert install for the proxy.
FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        curl \
        jq \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# tail -f /dev/null keeps the container alive so the host can `docker exec`
# into it for each agent shell command.
CMD ["tail", "-f", "/dev/null"]
