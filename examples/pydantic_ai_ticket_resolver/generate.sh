#!/usr/bin/env bash

set -euo pipefail

example_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repository_root="$(cd "${example_dir}/../.." && pwd)"
env_file="${example_dir}/.env"
trace_file="${example_dir}/traces/langfuse-traces.jsonl"

if [[ ! -f "${env_file}" ]]; then
  printf '%s\n' 'Create .env in the example directory and add your credentials first.' >&2
  exit 2
fi

cd "${repository_root}"

printf '%s\n' 'Generating 10 returns-resolution traces in Langfuse'
uv run --project "${example_dir}" --env-file "${env_file}" \
  python -m examples.pydantic_ai_ticket_resolver.generate_traces "${trace_file}"

printf 'Wrote %s\n' "${trace_file}"
