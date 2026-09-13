#!/usr/bin/env bash
set -euo pipefail

url="${DATA_URL:-https://zenodo.org/records/6382482/files/debs2022-gc-trading-day-08-11-21.csv?download=1}"
destination="${1:-data/debs2022-gc-trading-day-08-11-21.csv}"
partial="${destination}.part"

mkdir -p "$(dirname "$destination")"
if [[ -s "$destination" ]]; then
  echo "File already exists: $destination"
  exit 0
fi

echo "Downloading source data to $destination"
curl --fail --location --continue-at - --output "$partial" "$url"
mv "$partial" "$destination"