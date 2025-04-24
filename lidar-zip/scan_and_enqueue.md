# LiDAR Archive Scanner

## Overview
The LiDAR Archive Scanner is a Python-based application designed to scan directories containing LiDAR data and queue jobs for creating compressed archives. It utilizes Kubernetes for job management and SQLite for state tracking.

## Project Structure
```
lidar-archive-scanner
├── Dockerfile
├── scan_and_enqueue.py
├── persist_state.sql
├── kubernetes
│   ├── cronjob.yaml
│   └── pvcs.yaml
└── README.md
```

## Getting Started

### Prerequisites
- Docker
- Kubernetes cluster
- Python 3.x

### Building the Docker Image
To build the Docker image for the `scan_and_enqueue.py` script, navigate to the project directory and run the following command:

```bash
docker build -t lidar-archive-scanner .
```

### Running the Docker Image
You can run the Docker image locally for testing purposes with the following command:

```bash
docker run --rm \
  -v /path/to/lidar:/lidar \
  -v /path/to/lidar-zips:/lidar-zips \
  -v /path/to/state:/state \
  lidar-archive-scanner \
  uv run scan_and_enqueue.py --original-root ./lidar --zip-root ./lidar-zips --db-path ./state/lidar-archive.db --dry-run
```

### Deploying the CronJob
To deploy the Kubernetes CronJob, ensure you have the necessary Persistent Volume Claims (PVCs) defined in `kubernetes/pvcs.yaml`. Then, apply the CronJob configuration:

```bash
kubectl apply -f kubernetes/pvcs.yaml
kubectl apply -f kubernetes/cronjob.yaml
```

### Database Schema
The SQLite database schema is defined in `persist_state.sql`. This file contains the necessary SQL commands to create the `folder_state` table and any required indexes.

## Usage
The `scan_and_enqueue.py` script scans the specified directories for changes and queues jobs to create compressed archives. It supports different execution environments, including local, Docker, and Kubernetes.

## Logging
The application uses Python's built-in logging module to log information, warnings, and errors. Adjust the logging level through command-line arguments.

## Contributing
Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for details.