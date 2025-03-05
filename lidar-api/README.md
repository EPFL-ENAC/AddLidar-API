# Lidar API

This project is a FastAPI application that wraps the LidarDataManager CLI, allowing users to process LiDAR point cloud data via HTTP requests. The API runs the CLI inside a Docker container and mounts a volume for data access.

## Project Structure


```
lidar-api
├── src 
│ ├── main.py # Entry point of the FastAPI application 
│ ├── config 
│ │ └── settings.py # Configuration settings for the application 
│ ├── api 
│ │ ├── routes.py # API endpoints definition 
│ │ └── models.py # Data models for request and response 
│ ├── services 
│ │ └── docker_service.py # Logic for interacting with Docker 
│ └── utils 
│ └── file_utils.py # Utility functions for file handling 
├── kubernetes 
│ ├── deployment.yaml # Kubernetes deployment configuration 
│ └── service.yaml # Kubernetes service configuration 
├── Dockerfile # Instructions to build the Docker image 
├── requirements.txt # Python dependencies 
├── docker-compose.yml # Docker Compose configurations 
├── Makefile # Common development commands 
└── README.md # Project documentation
```

# Project documentation

## Requirements

- Python 3.11.5 or higher
- UV package manager
- Docker and Docker Compose
- Redis (for Persistence)

## Environment Variables

The application uses the following environment variables (defined in `settings.py`):

| Variable            | Default Value                       | Description                               |
|---------------------|-------------------------------------|-------------------------------------------|
| ENVIRONMENT         | development                         | Deployment environment                     |
| IMAGE_NAME          | ghcr.io/epfl-enac/lidardatamanager  | Docker image for LiDAR processing          |
| IMAGE_TAG           | latest                              | Docker image tag                           |
| ROOT_VOLUME         |                                     | Root volume path (based on PVC)            |
| NAMESPACE           | epfl-cryos-addlidar-potree-dev      | Kubernetes namespace                       |
| MOUNT_PATH          | /data                               | Container path for data mounting           |
| OUTPUT_PATH         | /output                             | Container path for output data             |
| PVC_OUTPUT_NAME     | lidar-data-output-pvc               | Output PVC name                            |
| PVC_NAME            | lidar-data-pvc                      | Input data PVC name                        |
| JOB_TIMEOUT         | 300                                 | Job timeout in seconds                     |
| DEFAULT_DATA_ROOT   | /data                               | Default root path for input data           |
| DEFAULT_OUTPUT_ROOT | /output                             | Default root path for output data          |

You can override these settings by creating a `.env` file in the project root.

## Setup Instructions

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd lidar-api

2. Install UV package manager if not already installed:
   ```
   pip install uv
   ```
3. Sync dependencies using UV:
   ```
   uv sync
   ```

3. Build the Docker image:
   ```
   make docker-build
   ```

4. Run the application using Docker Compose:
   ```
   docker-compose up
   ```

# Development
## Formatting Code
   ```
   make format
   ```
## Linting
   ```
   make lint
   ```
## Running Tests
   ```
   make test
   ```
## Security Scanning
   ```
   make scout
   ```


## Usage

### Endpoint

**GET /process-point-cloud**

Processes a LiDAR point cloud file using the CLI.

### Query Parameters

- `file_path` *(required)*: Path to the input point cloud file (inside mounted volume).
- `remove_attribute`: Remove specified attribute(s).
- `remove_all_attributes`: Remove all non-geometry attributes.
- `remove_color`: Remove color data.
- `format`: Output format (`pcd-ascii`, `lasv14`, etc.).
- `line`: Export a specific line index.
- `returns`: Max return index.
- `number`: Max number of points in output.
- `density`: Max density (points per m²).
- `roi`: Region of interest.
- `outcrs`: Output CRS (e.g., `EPSG:4326`).
- `incrs`: Override input CRS.

### Example Request

```bash
JSON='{"file_path": "/LiDAR/0001_Mission_Root/02_LAS_PCD/all_grouped_high_veg_10th_point.las", "outcrs": "EPSG:4326", "returns": 10, "format": "lasv14"}'

curl -G "http://localhost:8000/process-point-cloud" \
  -H "accept: application/json" \
  --data-urlencode "$(echo "$JSON" | jq -r 'to_entries | map("\(.key)=\(.value|@uri)") | join("&")')"
```

### Response

- **Success (`200 OK`)**: CLI output
- **Error (`400/500`)**: CLI error message

## License

This project is licensed under the GPLV3