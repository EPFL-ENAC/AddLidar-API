# LiDAR ZIP Archive Tool

This tool creates compressed archives for LiDAR data folders as part of the AddLidar project.

## Overview

The LiDAR ZIP Archive Tool is a containerized utility that helps package LiDAR datasets for easier distribution. It's designed to work with the AddLidar web-based system for storing, processing, and visualizing LiDAR datasets collected from airborne missions.

## Prerequisites

- Docker installed on your system
- Access to LiDAR data folders you want to compress
- GitHub Container Registry (ghcr.io) credentials (if pushing the image)

## Quick Start

### Building the Docker Image

```bash
# Build the Docker image
docker build -t ghcr.io/epfl-enac/lidar-zip:latest .
```

### Using the Tool

```bash
# Mount source LiDAR data and destination folders
docker run --rm \
  -v /path/to/your/lidar/data:/lidar:ro \
  -v /path/to/output/directory:/zips \
  ghcr.io/epfl-enac/lidar-zip:latest /lidar /zips
```

Replace `/path/to/your/lidar/data` with the location of your LiDAR data and `/path/to/output/directory` with where you want the ZIP files to be saved.

### Debugging

If you need to debug or explore the container:

```bash
# Run container with bash for debugging
docker run --rm -it \
  -v /path/to/your/lidar/data:/lidar:ro \
  -v /path/to/output/directory:/zips \
  --entrypoint /bin/bash \
  ghcr.io/epfl-enac/lidar-zip:latest
```

### Pushing to GitHub Container Registry

If you need to push the image to GitHub Container Registry:

```bash
# Log in to GitHub Container Registry
echo $GITHUB_TOKEN | docker login ghcr.io -u $GITHUB_USERNAME --password-stdin

# Push the image
docker push ghcr.io/epfl-enac/lidar-zip:latest
```

You'll need to set the environment variables `GITHUB_TOKEN` and `GITHUB_USERNAME` before running these commands.

### Using a Different Tag

If you want to use a specific version tag instead of `latest`:

```bash
# Build with a specific tag
docker build -t ghcr.io/epfl-enac/lidar-zip:v1.0.0 .

# Push with a specific tag
docker push ghcr.io/epfl-enac/lidar-zip:v1.0.0
```

## Integration with AddLidar

This tool is part of the AddLidar system, which is deployed on a Kubernetes cluster. It's designed to be run as a Kubernetes job for processing LiDAR datasets as part of the overall workflow.
