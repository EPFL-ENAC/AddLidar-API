# README.md

# README.md

# Lidar API

This project is a FastAPI application that wraps the LidarDataManager CLI, allowing users to process LiDAR point cloud data via HTTP requests. The API runs the CLI inside a Docker container and mounts a volume for data access.

## Project Structure

```
lidar-api
├── src
│   ├── main.py                # Entry point of the FastAPI application
│   ├── config
│   │   └── settings.py        # Configuration settings for the application
│   ├── api
│   │   ├── routes.py          # API endpoints definition
│   │   └── models.py          # Data models for request and response
│   ├── services
│   │   └── docker_service.py   # Logic for interacting with Docker
│   └── utils
│       └── file_utils.py      # Utility functions for file handling
├── kubernetes
│   ├── deployment.yaml         # Kubernetes deployment configuration
│   └── service.yaml            # Kubernetes service configuration
├── Dockerfile                  # Instructions to build the Docker image
├── requirements.txt           # Python dependencies
├── docker-compose.yml         # Docker Compose configurations
└── README.md                  # Project documentation
```

## Setup Instructions

1. Clone the repository:
   ```
   git clone <repository-url>
   cd lidar-api
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Build the Docker image:
   ```
   docker build -t lidar-api .
   ```

4. Run the application using Docker Compose:
   ```
   docker-compose up
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
# GET /process-point-cloud?file_path=/LiDAR/0001_Mission_Root/02_LAS_PCD/all_grouped_high_veg_10th_point.las&outcrs=EPSG:4326&r=10



curl -G "http://localhost:8000/process-point-cloud" \
  -H "accept: application/json" \
  --data-urlencode "$(echo "$JSON" | jq -r 'to_entries | map("\(.key)=\(.value|@uri)") | join("&")')"
```

### Response

- **Success (`200 OK`)**: CLI output
- **Error (`400/500`)**: CLI error message

## License

This project is licensed under the MIT License.