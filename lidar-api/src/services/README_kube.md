# Kubernetes LidarDataManager Wrapper

This module provides a Kubernetes-based implementation for processing LiDAR point cloud data using the LidarDataManager container.

## Overview

The wrapper creates Kubernetes jobs to run the LidarDataManager container for point cloud processing tasks. It handles:

- Creating Kubernetes jobs with flexible storage options
- Passing command line arguments to the container
- Managing output files
- Collecting logs and results
- Cleaning up completed jobs

## Requirements

- Kubernetes cluster access (configured via kubeconfig or in-cluster config)
- The LidarDataManager image available in the specified container registry

## Configuration

The module uses the following environment variables (with defaults):

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `IMAGE_NAME` | `ghcr.io/epfl-enac/lidardatamanager` | Container image name |
| `IMAGE_TAG` | `latest` | Container image tag |
| `NAMESPACE` | `default` | Kubernetes namespace |
| `MOUNT_PATH` | `/data` | Path to mount the data volume in container |
| `STORAGE_TYPE` | `emptyDir` | Storage type: `pvc`, `emptyDir`, `hostPath`, or `none` |
| `PVC_NAME` | `""` | Name of PVC (only used if `STORAGE_TYPE` is `pvc`) |
| `HOST_PATH` | `""` | Host path (only used if `STORAGE_TYPE` is `hostPath`) |
| `JOB_TIMEOUT` | `300` | Timeout in seconds for job completion |

### Storage Types

1. **emptyDir** (default): Uses Kubernetes ephemeral storage. Data is lost when the pod terminates.
2. **pvc**: Uses a Persistent Volume Claim (requires `PVC_NAME` to be set).
3. **hostPath**: Mounts a directory from the host node (requires `HOST_PATH` to be set).
4. **none**: No storage volumes mounted.

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

## Examples

### Using emptyDir (Default)

```bash
# No specific environment variables needed
python main_addlidarmanager.py
```

### Using Persistent Volume Claim

```bash
export STORAGE_TYPE="pvc"
export PVC_NAME="my-lidar-data-pvc"
python main_addlidarmanager.py
```

### Using Host Path

```bash
export STORAGE_TYPE="hostPath"
export HOST_PATH="/mnt/lidar-data"
python main_addlidarmanager.py
```

## Testing

Run the example in the main script to test functionality:

```bash
python main_addlidarmanager.py