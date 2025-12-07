#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE_DIR="${ROOT_DIR}/kafka-profile"
TAG="4.0.1"
BASE_URL="https://raw.githubusercontent.com/apache/kafka/${TAG}/clients/src/main/resources/common/message"

mkdir -p "${PROFILE_DIR}"

FILES=(
  ApiVersionsRequest.json
  ApiVersionsResponse.json
  MetadataRequest.json
  MetadataResponse.json
  ProduceRequest.json
  ProduceResponse.json
  FetchRequest.json
  FetchResponse.json
  ListOffsetsRequest.json
  ListOffsetsResponse.json
)

for file in "${FILES[@]}"; do
  echo "Fetching ${file}..."
  curl -fsSL "${BASE_URL}/${file}" -o "${PROFILE_DIR}/${file}"
  actual="$(sha256sum "${PROFILE_DIR}/${file}" | awk '{print $1}')"
  expected="$(jq -r --arg f "$file" '.files[] | select(.name==$f) | .sha256' "${PROFILE_DIR}/manifest.json")"
  if [ "$actual" != "$expected" ]; then
    echo "Checksum mismatch for ${file}: expected ${expected}, got ${actual}" >&2
    exit 1
  fi
done

echo "All schemas fetched successfully. Proceed with: zig build gen"