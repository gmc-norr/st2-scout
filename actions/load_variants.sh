#!/usr/bin/env bash

set -euo pipefail

SNV_VCF="$1"
PED="$2"
CONFIG="$3"

if [[ -z "${SNV_VCF}" ]]; then
  echo "Missing required parameter: vcf_file" >&2
  exit 1
fi

if [[ -z "${PED}" ]]; then
  echo "Missing required parameter: ped_file" >&2
  exit 1
fi

if [[ -z "${CONFIG}" ]]; then
  echo "Missing required parameter: config" >&2
  exit 1
fi

if [[ ! -f "${SNV_VCF}" ]]; then
  echo "vcf_file does not exist on remote host: ${SNV_VCF}" >&2
  exit 1
fi

if [[ ! -f "${PED}" ]]; then
  echo "ped_file does not exist on remote host: ${PED}" >&2
  exit 1
fi

docker compose run loqusdb-cli loqusdb -c "${CONFIG}" load --variant-file "${SNV_VCF}" --family-file "${PED}"