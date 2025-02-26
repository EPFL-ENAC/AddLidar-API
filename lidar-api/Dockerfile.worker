# Stage 1: Build dependencies
FROM python:3.11-slim-bookworm AS builder

# Copy uv binary from official UV image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set environment variables for uv
ENV UV_LINK_MODE=copy
ENV UV_COMPILE_BYTECODE=1
ENV UV_PROJECT_ENVIRONMENT=/app/.venv

# Create the app directory
WORKDIR /app

# Copy project files needed for dependency installation
COPY pyproject.toml uv.lock ./

# Install dependencies using uv sync
RUN uv sync --frozen --no-cache

# Stage 2: Final image
FROM python:3.11-slim-bookworm

# Install Docker client dependencies
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    gnupg \
    lsb-release \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
      $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update \
    && apt-get install -y docker-ce-cli \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy the uv binary from builder stage to final image
COPY --from=builder /usr/local/bin/uv /usr/local/bin/uv

# Copy the installed dependencies from the builder stage
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/uv.lock /app/uv.lock

# Create the app directory
WORKDIR /app

# Create data directory
RUN mkdir -p /app/data

# Copy the application code
COPY celery_worker.py .

# Set the path to include the virtual environment's binaries
ENV PATH="/app/.venv/bin:$PATH"

# Command to run Celery worker
CMD ["uv", "run", "celery", "-A", "celery_worker", "worker", "--loglevel=info"]