#!/usr/bin/env bash

set -euo pipefail

example_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repository_root="$(cd "${example_dir}/../.." && pwd)"
env_file="${repository_root}/.env"
prepared_trace_path="${example_dir}/traces/langfuse-traces.jsonl"
trace_path="${prepared_trace_path}"
import_only=false
bootstrap_traces=false

usage() {
  printf '%s\n' \
    "Usage: examples/document_processing/run.sh [OPTIONS]" \
    "" \
    "Options:" \
    "  --import-only          Stop after importing traces and creating cohorts." \
    "  --bootstrap-traces     Generate fresh PydanticAI and Langfuse traces first." \
    "  --trace-export PATH    Import another Langfuse JSON or JSONL export." \
    "  -h, --help             Show this help."
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --import-only)
      import_only=true
      shift
      ;;
    --bootstrap-traces)
      bootstrap_traces=true
      shift
      ;;
    --trace-export)
      if [[ $# -lt 2 ]]; then
        printf 'Missing path after --trace-export.\n' >&2
        exit 2
      fi
      trace_path="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "${bootstrap_traces}" == true && "${trace_path}" != "${prepared_trace_path}" ]]; then
  printf '%s\n' '--bootstrap-traces and --trace-export cannot be combined.' >&2
  exit 2
fi
if [[ ! -f "${env_file}" ]]; then
  printf '%s\n' 'Create .env from .env.example before running the example.' >&2
  exit 2
fi
if ! command -v jq >/dev/null 2>&1; then
  printf '%s\n' 'This example requires jq.' >&2
  exit 2
fi

run_label="$(date -u +%Y%m%d%H%M%S)-${RANDOM}"
cache_dir="${TMPDIR:-/tmp}/kitaru-document-processing-${run_label}"
mkdir -p "${cache_dir}"

cli() {
  uv run --env-file "${env_file}" \
    --extra cli \
    --extra worker \
    --extra pydantic-ai \
    kitaru --output json "$@"
}

worker_pid=''
stop_worker() {
  if [[ -n "${worker_pid}" ]] && kill -0 "${worker_pid}" 2>/dev/null; then
    kill -TERM "${worker_pid}" 2>/dev/null || true
    wait "${worker_pid}" 2>/dev/null || true
  fi
}
trap stop_worker EXIT INT TERM

if [[ "${bootstrap_traces}" == true ]]; then
  trace_path="${cache_dir}/langfuse-traces.jsonl"
  printf '%s\n' '-1/5 Generating fresh PydanticAI traces through Langfuse'
  uv run --env-file "${env_file}" python -m examples.document_processing.corpus
  uv run --env-file "${env_file}" \
    --extra pydantic-ai \
    --extra examples \
    python -m examples.document_processing.langfuse_capture "${trace_path}"
fi
if [[ ! -f "${trace_path}" ]]; then
  printf 'Trace export does not exist: %s\n' "${trace_path}" >&2
  exit 2
fi

agent_name="standards-extractor-${run_label}"
importer_name="langfuse-jsonl-${run_label}"
evaluator_name="document-field-accuracy-${run_label}"
experiment_name="standards-extractor-prompt-v2-${run_label}"
source_instance="nist-standards-${run_label}"
import_params="$(jq -cn --arg source "${source_instance}" '{source_instance:$source}')"

printf '%s\n' '1/5 Testing and registering the agent and importer'
cli importer test \
  "${repository_root}/plugins/langfuse/src/kitaru_importer_langfuse/importer.py" \
  --entrypoint parse \
  --payload "${trace_path}" \
  --params "${import_params}" >/dev/null
cli agent register "${agent_name}" \
  --entrypoint examples.document_processing.agent:main \
  --description 'Extract catalog fields from standards PDFs.' \
  --display-version prompt-v2 \
  --working-dir "${repository_root}" \
  --timeout-seconds 180 >/dev/null
cli importer register "${importer_name}" \
  --script "${repository_root}/plugins/langfuse/src/kitaru_importer_langfuse/importer.py" \
  --entrypoint parse \
  --provider langfuse \
  --description 'Import Langfuse traces and PydanticAI observations.' \
  --display-version 0.1.0 >/dev/null

uv run --env-file "${env_file}" \
  --extra cli \
  --extra worker \
  --extra pydantic-ai \
  kitaru --output jsonl worker start \
  --name "document-example-${run_label}" \
  --kinds agent \
  --kinds evaluator \
  --kinds importer \
  --concurrency 4 \
  --poll-interval 0.1 \
  --blob-cache-root "${cache_dir}/blobs" \
  --payload-cache-root "${cache_dir}/payloads" >"${cache_dir}/worker.jsonl" &
worker_pid=$!
for _ in {1..20}; do
  if ! kill -0 "${worker_pid}" 2>/dev/null; then
    printf '%s\n' 'The example worker stopped during startup.' >&2
    tail -20 "${cache_dir}/worker.jsonl" >&2
    exit 1
  fi
  if grep -q '"event":"starting"' "${cache_dir}/worker.jsonl"; then
    break
  fi
  sleep 0.1
done

printf '2/5 Importing traces from %s\n' "${trace_path}"
cli session import "${trace_path}" \
  --importer "${importer_name}@1" \
  --agent "${agent_name}" \
  --params "${import_params}" \
  --media-type application/x-ndjson \
  --wait \
  --interval 0.2 \
  --timeout 300 >/dev/null

sessions_json="$(
  cli session list \
    --agent "${agent_name}" \
    --origin imported \
    --size 1000
)"
session_count="$(jq '.items | length' <<<"${sessions_json}")"
if [[ "${trace_path}" == "${prepared_trace_path}" && "${session_count}" -ne 12 ]]; then
  printf 'Expected 12 imported sessions, received %s.\n' "${session_count}" >&2
  exit 1
fi
if [[ "${session_count}" -eq 0 ]]; then
  printf '%s\n' 'The trace export did not contain any sessions.' >&2
  exit 1
fi

create_cohort() {
  local cohort_label="$1"
  local tag="$2"
  local ids
  local cohort_json
  local cohort_id
  local version_json
  local -a membership_args=()

  if [[ -z "${tag}" ]]; then
    ids="$(jq -r '.items[].id' <<<"${sessions_json}")"
  else
    ids="$(
      jq -r --arg tag "${tag}" \
        '.items[] | select((.metadata["langfuse.tags"] // []) | index($tag)) | .id' \
        <<<"${sessions_json}"
    )"
  fi
  if [[ -z "${ids}" ]]; then
    return
  fi
  cohort_json="$(
    cli cohort create "${cohort_label}-${run_label}" \
      --agent "${agent_name}" \
      --description "Imported document traces for ${cohort_label}."
  )"
  cohort_id="$(jq -r '.item.id' <<<"${cohort_json}")"
  while IFS= read -r session_id; do
    membership_args+=(--add-session "${session_id}")
  done <<<"${ids}"
  version_json="$(
    cli cohort version create "${cohort_id}" \
      "${membership_args[@]}" \
      --display-version import-v1
  )"
  printf '  %-24s %2d sessions\n' "${cohort_label}" "$(wc -l <<<"${ids}")" >&2
  printf '%s' "${version_json}"
}

printf '%s\n' 'Creating cohorts from imported Langfuse tags'
create_cohort all-imported '' >/dev/null
create_cohort controls control >/dev/null
extraction_cohort_json="$(create_cohort extraction-edge-cases extraction-edge)"
create_cohort telemetry-edge-cases telemetry-edge >/dev/null

if [[ "${import_only}" == true ]]; then
  printf '%s\n' '' 'Import complete. Re-run without --import-only to replay a cohort.'
  exit 0
fi
if [[ -z "${extraction_cohort_json}" ]]; then
  printf '%s\n' 'No extraction-edge cohort was created.' >&2
  exit 1
fi
extraction_cohort_id="$(jq -r '.item.id' <<<"${extraction_cohort_json}")"

printf '%s\n' '3/5 Downloading PDFs and registering field evaluation'
uv run --env-file "${env_file}" \
  --extra pydantic-ai \
  python -m examples.document_processing.corpus
cli evaluator test "${example_dir}/evaluator.py" --entrypoint evaluate >/dev/null
cli evaluator register "${evaluator_name}" \
  --script "${example_dir}/evaluator.py" \
  --entrypoint evaluate \
  --description 'Compare extracted fields with reviewed labels.' \
  --display-version 1.0 >/dev/null

printf '%s\n' '4/5 Creating and running the CLI experiment'
cli experiment create "${experiment_name}" \
  --description 'Compare the imported baselines with the revised extractor.' \
  --evaluator "${evaluator_name}@1" >/dev/null
run_json="$(
  cli experiment run start "${experiment_name}" \
    --cohort-version "${extraction_cohort_id}" \
    --agent "${agent_name}@1" \
    --evaluate-baselines \
    --wait \
    --interval 0.5 \
    --timeout 1800
)"

printf '%s\n' '5/5 Experiment completed'
printf 'Experiment run: %s\n' "$(jq -r '.item.run.id' <<<"${run_json}")"
