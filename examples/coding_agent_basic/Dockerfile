FROM --platform=linux/amd64 strickvl/kitaru-dev:latest

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
