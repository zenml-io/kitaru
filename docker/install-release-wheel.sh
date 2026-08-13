#!/bin/sh
set -eu

attempt=1
max_attempts=${KITARU_INSTALL_ATTEMPTS:-10}
retry_delay=${KITARU_INSTALL_RETRY_DELAY:-15}

while ! uv pip install \
  --no-deps \
  --only-binary=:all: \
  --exclude-newer-package "kitaru=0 days" \
  --refresh-package kitaru \
  "kitaru==$KITARU_VERSION"
do
  if [ "$attempt" -ge "$max_attempts" ]; then
    exit 1
  fi
  echo "Kitaru $KITARU_VERSION is not available yet; retrying"
  sleep "$retry_delay"
  attempt=$((attempt + 1))
done
