#!/usr/bin/env bash

set -euo pipefail

CASE_ID="$1"
VCF_PATH="$2"
CANCER="$3"
SV="$4"

if [[ -z "${CASE_ID}" ]]; then
  echo "Missing required parameter: case_id" >&2
  exit 1
fi

if [[ ! -f "${VCF_PATH}" ]]; then
  echo "VCF file does not exist on remote host: ${VCF_PATH}" >&2
  exit 1
fi

if [[ "$CANCER" == "1" ]]; then
  if [[ "$SV" == "1" ]]; then
    UPDATE_OPTION="--vcf-cancer-sv"
  else
    UPDATE_OPTION="--vcf-cancer"
  fi
else
  if [[ "$SV" == "1" ]]; then
    UPDATE_OPTION="--vcf-sv"
  else
    UPDATE_OPTION="--vcf"
  fi
fi

docker compose exec scout scout update case "${UPDATE_OPTION}" "${VCF_PATH}" "${CASE_ID}"
