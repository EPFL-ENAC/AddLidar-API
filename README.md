# ADDLidar API

## Specifications
This API provides a RESTful interface for processing LiDAR point cloud data. Built with FastAPI and Docker, it offers various data manipulation capabilities.

### Tech Stack
- Python 3.9
- FastAPI
- Docker
- Kubernetes (for deployment)
- UV package manager

### Requirements
- Docker installed
- Python 3.9+
- Make (optional, for using Makefile commands)

### Development Setup
1. Clone the repository
2. Install dependencies: `make install` or `uv pip install -r requirements.txt`
3. Create a `.env` file: `cp .env.example .env` and adjust values as needed
4. Run locally: `make run` or `uvicorn src.main:app --reload`

### Environment Configuration
Create a `.env` file in the root directory with the following variables:
```bash
ENVIRONMENT=development
DOCKER_IMAGE=ghcr.io/epfl-enac/lidardatamanager:latest
DOCKER_VOLUME=/path/to/your/data/folder
API_PREFIX=/api
PORT=8000
```

### API Endpoints
- `GET /process-point-cloud`: Process point cloud data with customizable parameters

### Docker Usage
```bash
docker build -t lidar-api .
docker run -p 8000:8000 lidar-api
```

### Testing
Run tests with: `make test` or `pytest`

### Code Quality
- Linting: `make lint`
- Formatting: `make format`


# Todo when setting up your github repo

- [x] Learn how to use github template repository: https://docs.github.com/en/repositories/creating-and-managing-repositories/creating-a-repository-from-a-template
- [ ] Activate discussion (https://github.com/EPFL-ENAC/AddLidar-API/settings)
- [x] Replace `AddLidar-API` by the name of your repo
- [x] Modifiy or remove the `CITATION.cff` file. [How to format it ?](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-citation-files) 
- [ ] Check if you need all those labels: https://github.com/EPFL-ENAC/AddLidar-API/labels
- [ ] Create your first milestone: https://github.com/EPFL-ENAC/AddLidar-API/milestones
- [ ] Protect your branch if you're a pro user: https://github.com/EPFL-ENAC/AddLidar-API/settings/branches




http://0.0.0.0:8000/process-point-cloud?file_path=%2FLiDAR%2F0001_Mission_Root%2F02_LAS_PCD%2Fall_grouped_high_veg_10th_point.las&outcrs=EPSG%3A4326&returns=10&format=lasv14

