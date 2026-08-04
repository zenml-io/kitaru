#!/usr/bin/env bash

set -euo pipefail

example_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repository_root="$(cd "${example_dir}/../.." && pwd)"
env_file="${example_dir}/.env"
trace_file="${example_dir}/traces/langfuse-traces.jsonl"

if [[ ! -f "${env_file}" ]]; then
  printf '%s\n' 'Copy .env.example to .env first.' >&2
  exit 2
fi

cd "${repository_root}"

printf '%s\n' 'Downloading three NIST PDFs'
uv run --env-file "${env_file}" \
  python -m examples.document_processing.corpus

printf '%s\n' 'Generating real PydanticAI traces in Langfuse'
uv run --env-file "${env_file}" \
  --extra pydantic-ai \
  --extra examples \
  python -m examples.document_processing.langfuse_capture "${trace_file}"

printf 'Wrote %s\n' "${trace_file}"
