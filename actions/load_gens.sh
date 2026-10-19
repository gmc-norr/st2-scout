#!/usr/bin/env bash

set -euo pipefail

BAF="$1"
COV="$2"
CASE_ID="$3"
SAMPLE_ID="$4"
SEX="$5"


if [[ -z "${BAF}" ]]; then
  echo "Missing required parameter: baf.bed file" >&2
  exit 1
fi

if [[ -z "${COV}" ]]; then
  echo "Missing required parameter: cov.bed file" >&2
  exit 1
fi

if [[ -z "${CASE_ID}" ]]; then
  echo "Missing required parameter: case_id" >&2
  exit 1
fi

if [[ -z "${SAMPLE_ID}" ]]; then
  echo "Missing required parameter: sample_id" >&2
  exit 1
fi

if [[ -z "${SEX}" ]]; then
  echo "Missing required parameter: sex" >&2
  exit 1
fi

if [[ ! -f "${BAF}" ]]; then
  echo "baf file does not exist on remote host: ${BAF}" >&2
  exit 1
fi

if [[ ! -f "${COV}" ]]; then
  echo "cov file does not exist on remote host: ${COV}" >&2
  exit 1
fi

docker compose exec gens gens load sample --baf "${BAF}" --coverage "${COV}" --case-id  "${CASE_ID}" --sample-id "${SAMPLE_ID}" --sex "${SEX}" --sample-type proband --genome-build 38