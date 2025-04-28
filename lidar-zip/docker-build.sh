#!/bin/bash
# Script to build and push the Docker image

# Set your registry and image details
REGISTRY="ghcr.io"
IMAGE_NAME="epfl-enac/lidar-zip"
IMAGE_TAG="latest"

# Build the image
echo "Building Docker image..."
docker build -t "${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}" .


# Ask before pushing
read -p "Do you want to push the image to ${REGISTRY}? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Pushing image to registry..."
    docker push "${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
else
    echo "Skipping push to registry"
fi

echo "Image ${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG} built successfully"