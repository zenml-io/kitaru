#!/usr/bin/env bash
# One-time setup for stages 4+.
#
# Builds the proxy + mock-services Docker images and creates the
# kitaru secret the agent's wiki proxy rule references. Idempotent —
# safe to run multiple times.

set -euo pipefail

cd "$(dirname "$0")"

echo ">> Building agent-factory-proxy image"
docker build -t agent-factory-proxy -f docker/proxy.Dockerfile .

echo ">> Building agent-factory-mock image"
docker build -t agent-factory-mock -f docker/mock.Dockerfile .

echo ">> Setting kitaru secret 'wiki-token' (idempotent — set creates or updates)"
kitaru secrets set wiki-token --value=wiki-token

echo ">> Setup complete. Run: python stage_4_credential_proxy.py"
