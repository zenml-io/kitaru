#!/usr/bin/env bash
# One-time setup for stages 2+.
#
# Builds the sandbox + proxy + mock-services Docker images and creates
# the kitaru secrets the agent's services / proxy rules reference.
# Idempotent — safe to run multiple times.

set -euo pipefail

cd "$(dirname "$0")"

echo ">> Building agent-harness-platform-sandbox image (used from stage 2 onward)"
docker build -t agent-harness-platform-sandbox -f docker/sandbox.Dockerfile docker/

echo ">> Building agent-harness-platform-proxy image (used from stage 4 onward)"
docker build -t agent-harness-platform-proxy -f docker/proxy.Dockerfile .

echo ">> Building agent-harness-platform-mock image (used from stage 4 onward)"
docker build -t agent-harness-platform-mock -f docker/mock.Dockerfile .

echo ">> Setting kitaru secret 'wiki-token' (idempotent — set creates or updates)"
kitaru secrets set wiki-token --value=wiki-token

echo ">> Setting kitaru secret 'webhook-token' (used from stage 5 onward)"
kitaru secrets set webhook-token --value=webhook-token

echo ">> Setup complete."
