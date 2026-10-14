#!/usr/bin/env bash

set -euo pipefail

CASE_ID="$1"

if [[ -z "${CASE_ID}" ]]; then
  echo "Missing required parameter: case_id" >&2
  exit 1
fi

if [[ ! -f "${CASE_ID}" ]]; then
  echo "Config file does not exist on remote host: ${CASE_ID}" >&2
  exit 1
fi

docker compose exec scout scout export cases --json --case-id "${CASE_ID}"
