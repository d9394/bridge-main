#!/bin/bash
set -e

IMAGE_NAME="ilink-bridge"
TAG="${1:-v2.2.0}"

echo "Building ${IMAGE_NAME}:${TAG}..."
docker build -t "${IMAGE_NAME}:${TAG}" .

echo ""
echo "Done. Image size:"
docker images "${IMAGE_NAME}:${TAG}" --format "  {{.Size}}"
