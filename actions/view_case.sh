#!/usr/bin/env bash

set -euo pipefail

DISPLAY_NAME="$1"

if [[ -z "${DISPLAY_NAME}" ]]; then
  echo "Missing required parameter: display_name" >&2
  exit 1
fi

docker compose exec scout scout view case -d "${DISPLAY_NAME}"
