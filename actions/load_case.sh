#!/usr/bin/env bash
set -euo pipefail

LOAD_CONFIG=""
CONTAINER_NAME="scout"

for arg in "$@"; do
  case "$arg" in
    --load_config=*)
      LOAD_CONFIG="${arg#*=}"
      ;;
    --container_name=*)
      CONTAINER_NAME="${arg#*=}"
      ;;
  esac
done

if [[ -z "${LOAD_CONFIG}" ]]; then
  echo "Missing required parameter: load_config" >&2
  exit 1
fi

if [[ ! -f "${LOAD_CONFIG}" ]]; then
  echo "Config file does not exist on remote host: ${LOAD_CONFIG}" >&2
  exit 1
fi

docker exec "${CONTAINER_NAME}" scout load case "${LOAD_CONFIG}"