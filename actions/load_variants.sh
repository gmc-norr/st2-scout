#!/usr/bin/env bash

set -euo pipefail

VARIANTS="$1"

if [[ -z "${VARIANTS}" ]]; then
  echo "Missing required parameter: variants" >&2
  exit 1
fi

if [[ ! -f "${VARIANTS}" ]]; then
  echo "variants does not exist on remote host: ${VARIANTS}" >&2
  exit 1
fi

docker compose exec loqusdb load "${VARIANTS}"