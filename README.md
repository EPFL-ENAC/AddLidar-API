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
- Kubernetes for job creation
- Python 3.9+
- Make (optional, for using Makefile commands)

### Development
- Follow the [lidar-api/README.md](lidar-api/README.md)

### Git Hooks
This project uses [lefthook](https://github.com/evilmartians/lefthook) for managing Git hooks.

#### Setup
To set up the pre-commit hooks that automatically run formatting and linting before each commit:

```bash
# Install lefthook and initialize Git hooks
make setup-hooks
```

This will:
1. Install lefthook globally via npm
2. Set up the Git hooks configuration

#### What the hooks do
- **Pre-commit**: Automatically runs `make format` and `make lint` on Python files to ensure code quality before committing

#### Skipping hooks
If you need to bypass the hooks for a specific commit:
```bash
git commit --no-verify
```