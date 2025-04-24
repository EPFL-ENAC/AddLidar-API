FROM python:3.9-alpine

# Set the working directory
WORKDIR /app

# Copy the necessary files into the container
COPY scan_and_enqueue.py .
COPY persist_state.sql .

# Install required dependencies
RUN apk add --no-cache \
    py3-pip \
    && pip install --no-cache-dir \
    kubernetes \
    pydantic

# Command to run the script with the specified arguments
CMD ["python", "scan_and_enqueue.py", "--original-root", "./lidar", "--zip-root", "./lidar-zips", "--db-path", "./state/lidar-archive.db", "--dry-run"]