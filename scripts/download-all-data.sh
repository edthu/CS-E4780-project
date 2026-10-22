#!/usr/bin/env bash
set -euo pipefail

TARGET_DIR="${1:-data}"
BASE_URL="${DATA_URL_BASE:-https://zenodo.org/records/6382482/files}"

mkdir -p "$TARGET_DIR"

DAYS=("08" "09" "10" "11" "12" "13" "14")

echo "==> Downloading DEBS 2022 datasets for 08-11-21 to 14-11-21 into '$TARGET_DIR'"

for day in "${DAYS[@]}"; do
  filename="debs2022-gc-trading-day-${day}-11-21.csv"
  destination="${TARGET_DIR}/${filename}"
  partial="${destination}.part"
  url="${BASE_URL}/${filename}?download=1"

  if [[ -s "$destination" ]]; then
    echo "File already exists: $destination (skipping)"
    continue
  fi

  echo "Downloading $filename to $destination"
  curl --fail --location --continue-at - --output "$partial" "$url"
  mv "$partial" "$destination"
  echo "Downloaded $destination"
done

echo "==> All datasets from 08-11-21 to 14-11-21 are present in '$TARGET_DIR'."

