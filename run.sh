#!/bin/bash
set -e

IMAGE_NAME="ilink-bridge"
TAG="${1:-v2.0.0}"
CONTAINER_NAME="ilink-bridge"
SCRIPT_DIR="$(dirname "$0")"
STORAGE_DIR="${SCRIPT_DIR}/.ilink-bridge"

mkdir -p "${STORAGE_DIR}/auth" "${STORAGE_DIR}/logs" "${STORAGE_DIR}/cache/files"

if docker ps -aq -f name="${CONTAINER_NAME}" | grep -q .; then
    echo "Stopping existing container..."
    docker stop "${CONTAINER_NAME}" 2>/dev/null || true
    docker rm "${CONTAINER_NAME}" 2>/dev/null || true
fi

docker run -d \
    --name "${CONTAINER_NAME}" \
    --restart unless-stopped \
    -p 8765:8765 \
    -p 8086:8086 \
    -v "${SCRIPT_DIR}/config.yaml:/app/config.yaml:ro" \
    -v "${SCRIPT_DIR}/ilink_config.yaml:/app/ilink_config.yaml:ro" \
    -v "${SCRIPT_DIR}/synology_config.yaml:/app/synology_config.yaml:ro" \
    -v "${STORAGE_DIR}:/app/.ilink-bridge" \
    -v "${SCRIPT_DIR}/core:/app/core" \
    -v "${SCRIPT_DIR}/utils:/app/utils" \
    -v "${SCRIPT_DIR}/commands:/app/commands" \
    -v "${SCRIPT_DIR}/downstream:/app/downstream" \
    -v "${SCRIPT_DIR}/upstream:/app/upstream" \
    -v "${SCRIPT_DIR}/security:/app/security" \
    -v "${SCRIPT_DIR}/plugins/synology_plugin:/app/plugins/synology_plugin" \
    -e TZ=Asia/Shanghai \
    "${IMAGE_NAME}:${TAG}"

echo ""
echo "Container started."
