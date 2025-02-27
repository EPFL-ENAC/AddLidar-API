# Kubernetes LidarDataManager Wrapper

This module provides a Kubernetes-based implementation for processing LiDAR point cloud data using the LidarDataManager container.

## Overview

The wrapper creates Kubernetes jobs to run the LidarDataManager container for point cloud processing tasks. It handles:

- Creating Kubernetes jobs with proper volume mounts
- Passing command line arguments to the container
- Managing output files
- Collecting logs and results
- Cleaning up completed jobs

## Requirements

- Kubernetes cluster access (configured via kubeconfig)
- Persistent Volume Claim (PVC) for data storage
- The LidarDataManager image available in the specified container registry

## Configuration

The module uses the following environment variables (with defaults):

- `IMAGE_NAME`: Container image name (default: "ghcr.io/epfl-enac/lidardatamanager")
- `IMAGE_TAG`: Container image tag (default: "latest")
- `NAMESPACE`: Kubernetes namespace (default: "default")
- `PVC_NAME`: Name of PVC for data storage (default: "lidar-data-pvc")
- `MOUNT_PATH`: Path to mount the data volume (default: "/data")

## Usage

```python
from kubernetes_api_wrapper.main_addlidarmanager import process_point_cloud

# Prepare CLI arguments for LidarDataManager
cli_args = [
    "-i=/data/LiDAR/0001_Mission_Root/02_LAS_PCD/sample.las",
    "--format=pcd-ascii",
    "--remove-color"
]

# Process the point cloud
output, exit_code, output_file_path = process_point_cloud(cli_args)

if exit_code == 0:
    print(f"Processing successful! Output file: {output_file_path}")
else:
    print(f"Processing failed with error: {output.decode('utf-8')}")
```

## Kubernetes Setup

Before using this wrapper, ensure you have:

1. A Kubernetes cluster with appropriate access
2. A Persistent Volume and Persistent Volume Claim for data storage:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: lidar-data-pvc
  namespace: default
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 10Gi
```

3. The LidarDataManager image available in your container registry

## Testing

Run the example in the main script to test functionality:

```bash
python main_addlidarmanager.py
```