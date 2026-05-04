#!/usr/bin/env bash
# One-time setup for stages 2+.
#
# Builds the sandbox + proxy + mock-services Docker images and creates
# the kitaru secrets the agent's services / proxy rules reference.
# Idempotent — safe to run multiple times.

set -euo pipefail

cd "$(dirname "$0")"

echo ">> Building agent-factory-sandbox image (used from stage 2 onward)"
docker build -t agent-factory-sandbox -f docker/sandbox.Dockerfile docker/

echo ">> Building agent-factory-proxy image (used from stage 4 onward)"
docker build -t agent-factory-proxy -f docker/proxy.Dockerfile .

echo ">> Building agent-factory-mock image (used from stage 4 onward)"
docker build -t agent-factory-mock -f docker/mock.Dockerfile .

echo ">> Setting kitaru secret 'wiki-token' (idempotent — set creates or updates)"
kitaru secrets set wiki-token --value=wiki-token

echo ">> Setting kitaru secret 'webhook-token' (used from stage 5 onward)"
kitaru secrets set webhook-token --value=webhook-token

echo ">> Setup complete."
