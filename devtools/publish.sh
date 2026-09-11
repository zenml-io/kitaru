#!/usr/bin/env bash
# Build and push dev Kitaru server and worker images, push a dev Helm chart,
# and print or create the workspace payload for ZenML Pro.
#
# Per-user settings live in devtools/publish.env, see publish.env.example.
# Usage: devtools/publish.sh [--deploy]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/publish.env"

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Missing $CONFIG_FILE, copy publish.env.example and fill it in" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$CONFIG_FILE"

DEPLOY=false
for arg in "$@"; do
  case "$arg" in
    --deploy) DEPLOY=true ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 1
      ;;
  esac
done

require() {
  local name
  for name in "$@"; do
    if [[ -z "${!name:-}" ]]; then
      echo "$name is not set in $CONFIG_FILE" >&2
      exit 1
    fi
  done
}

require AWS_PROFILE IMAGE_REPOSITORY_PREFIX OWNER_ID ORGANIZATION_ID
if [[ "$DEPLOY" == true ]]; then
  require ZENML_PRO_PAT
fi
ZENML_PRO_URL="${ZENML_PRO_URL:-https://staging.cloud.zenml.io}"
ZENML_PRO_API_URL="${ZENML_PRO_API_URL:-https://staging.cloudapi.zenml.io}"

cd "$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"

HASH="$(git rev-parse --short HEAD)"
VERSION="$(uv version --short)"
TAG="${TAG:-dev-$HASH}"
# Replace the +dev build metadata with a prerelease suffix because OCI tags
# cannot contain a plus sign.
CHART_VERSION="${CHART_VERSION:-${VERSION%%+*}-dev.$HASH}"
SERVER_IMAGE="$IMAGE_REPOSITORY_PREFIX/kitaru-server"
WORKER_IMAGE="$IMAGE_REPOSITORY_PREFIX/kitaru-worker"

echo "TAG=$TAG"
echo "CHART_VERSION=$CHART_VERSION"

aws sts get-caller-identity --profile "$AWS_PROFILE" >/dev/null || {
  echo "$AWS_PROFILE SSO session missing or expired" >&2
  exit 1
}

aws ecr-public get-login-password --region us-east-1 --profile "$AWS_PROFILE" \
  | helm registry login --username AWS --password-stdin public.ecr.aws

docker build --platform linux/amd64 -f docker/dev-server.Dockerfile --target runtime \
  -t "$SERVER_IMAGE:$TAG" .
docker push "$SERVER_IMAGE:$TAG"

# Build the Kitaru wheel plus one wheel per plugin package into
# plugins/candidate-wheels. The candidate worker image ships that directory as
# UV_FIND_LINKS so plugin requirements resolve against the local wheels.
find plugins/candidate-wheels -maxdepth 1 -type f -name '*.whl' -delete
uv run --no-sync python scripts/smoke_plugin_artifacts.py --candidate-dir plugins/candidate-wheels

docker build --platform linux/amd64 -f plugins/candidate.Dockerfile --target worker \
  --build-arg KITARU_VERSION="$VERSION" \
  --build-arg "KITARU_EXTRAS=--extra worker" \
  -t "$WORKER_IMAGE:$TAG" .
docker push "$WORKER_IMAGE:$TAG"

helm lint ./helm
CHART_DIST="dist"
mkdir -p "$CHART_DIST"
helm package ./helm --version "$CHART_VERSION" --destination "$CHART_DIST"
helm push "$CHART_DIST/kitaru-$CHART_VERSION.tgz" oci://public.ecr.aws/zenml

PAYLOAD=$(cat <<EOF
{
  "name": "$TAG",
  "owner_id": "$OWNER_ID",
  "organization_id": "$ORGANIZATION_ID",
  "workspace_type": "kitaru",
  "kitaru_service": {
    "configuration": {
      "admin": {
        "helm_chart_version": "$CHART_VERSION",
        "image_repository": "$SERVER_IMAGE",
        "image_tag": "$TAG",
        "environment_vars": {
          "KITARU_SERVER_EPHEMERAL_WORKER__IMAGE": "$WORKER_IMAGE:$TAG"
        }
      }
    }
  }
}
EOF
)

echo "$PAYLOAD"

if [[ "$DEPLOY" == true ]]; then
  RESPONSE=$(curl -s --fail-with-body -X POST "$ZENML_PRO_API_URL/workspaces" \
    -H 'accept: application/json' \
    -H "Authorization: Bearer $ZENML_PRO_PAT" \
    -H 'Content-Type: application/json' \
    -d "$PAYLOAD") || {
    echo "Failed to create workspace: $RESPONSE" >&2
    exit 1
  }
  WORKSPACE_NAME=$(echo "$RESPONSE" | jq -r '.name')
  echo "Workspace created: $ZENML_PRO_URL/workspaces/$WORKSPACE_NAME"
fi
