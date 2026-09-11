#!/usr/bin/env bash

set -euo pipefail

GENE_PANEL_FILE="$1"

if [[ -z "${GENE_PANEL_FILE}" ]]; then
  echo "Missing required parameter: gene_panel_file" >&2
  exit 1
fi

if [[ ! -f "${GENE_PANEL_FILE}" ]]; then
  echo "Gene panel file does not exist on remote host: ${GENE_PANEL_FILE}" >&2
  exit 1
fi

docker compose exec scout scout load panel "${GENE_PANEL_FILE}"
