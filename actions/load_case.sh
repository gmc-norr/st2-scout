#!/usr/bin/env bash

set -euo pipefail

LOAD_CONFIG="$1"

if [[ -z "${LOAD_CONFIG}" ]]; then
  echo "Missing required parameter: load_config" >&2
  exit 1
fi

if [[ ! -f "${LOAD_CONFIG}" ]]; then
  echo "Config file does not exist on remote host: ${LOAD_CONFIG}" >&2
  exit 1
fi

docker compose exec scout scout load case "${LOAD_CONFIG}"
