"""
Celery worker for processing LiDAR point cloud data asynchronously
"""
from celery import Celery
import docker
import os
import logging
import time
from typing import Dict, Optional, List, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Setup Celery with environment variables for configuration
celery_app = Celery(
    "lidar_tasks",
    broker=os.environ.get("CELERY_BROKER_URL", "redis://redis:6379/0"),
    backend=os.environ.get("CELERY_RESULT_BACKEND", "redis://redis:6379/0"),
)

# Celery configuration
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    worker_hijack_root_logger=False,
)


def pull_image_with_retry(
    client: docker.DockerClient, image_name: str, image_tag: str, max_retries: int = 3
) -> bool:
    """
    Pull a Docker image with retry logic

    Args:
        client: Docker client instance
        image_name: Name of the Docker image
        image_tag: Tag of the Docker image
        max_retries: Maximum number of retry attempts

    Returns:
        Boolean indicating success or failure
    """
    retry_count = 0
    while retry_count < max_retries:
        try:
            logger.info(
                f"Pulling image {image_name}:{image_tag}, attempt {retry_count + 1}"
            )
            client.images.pull(image_name, tag=image_tag)
            logger.info(f"Successfully pulled image {image_name}:{image_tag}")
            return True
        except docker.errors.APIError as e:
            retry_count += 1
            auth_error = "denied: denied" in str(e)
            if auth_error:
                logger.warning(
                    f"Authentication error while pulling image. Image may be private or registry settings incorrect: {e}"
                )
                # If this is a public image but we're having auth issues,
                # let's check if the image already exists locally
                try:
                    client.images.get(f"{image_name}:{image_tag}")
                    logger.info(
                        f"Image {image_name}:{image_tag} found locally, proceeding without pulling"
                    )
                    return True
                except docker.errors.ImageNotFound:
                    pass
            if retry_count >= max_retries:
                logger.error(f"Failed to pull image after {max_retries} attempts: {e}")
                return False
            wait_time = 2**retry_count  # Exponential backoff
            logger.info(f"Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    return False


@celery_app.task(bind=True, name="process_lidar_data", max_retries=3)
def process_lidar_data(
    self,
    job_id: str,
    input_file: str,
    parameters: Optional[List[str]] = None,
    image_name: str = os.environ.get(
        "IMAGE_NAME", "ghcr.io/epfl-enac/lidardatamanager"
    ),
    image_tag: str = os.environ.get("IMAGE_TAG", "latest"),
) -> Dict[str, Any]:
    """
    Process LiDAR data using a Docker container

    Args:
        job_id: Unique identifier for the job
        input_file: Path to the input LiDAR file
        parameters: CLI parameters to pass to the processing container
        image_name: Docker image name for LiDAR processing
        image_tag: Docker image tag

    Returns:
        Dictionary containing job results and metadata
    """
    logger.info(f"Processing job {job_id} with file {input_file}")

    try:
        # Initialize Docker client
        client = docker.from_env()

        # Try to pull the image with retry logic
        if not pull_image_with_retry(client, image_name, image_tag):
            error_msg = f"Failed to pull Docker image {image_name}:{image_tag}"
            logger.error(error_msg)
            return {"status": "error", "job_id": job_id, "error": error_msg}

        # Set up volume paths - use ROOT_VOLUME from env if available, otherwise use default
        host_data_dir = os.environ.get("ROOT_VOLUME", os.environ.get("HOST_DATA_DIR", "/app/data"))
        container_data_dir = "/data"
        
        logger.info(f"Using host data directory: {host_data_dir}")
        
        # Ensure output directory exists
        output_dir = os.path.join(host_data_dir, "output")
        os.makedirs(output_dir, exist_ok=True)

        # Default parameters if none provided
        if parameters is None:
            parameters = []

        # Process all input paths to ensure they use the container path (/data)
        processed_parameters = []
        for param in parameters:
            # Check if this is an input file argument (-i=something)
            if param.startswith("-i="):
                input_path = param[3:]  # Extract the path part
                
                # If the path already starts with /data, use it as is
                if input_path.startswith("/data/"):
                    processed_parameters.append(param)
                # If it's an absolute path from the host data directory
                elif os.path.isabs(input_path) and input_path.startswith(host_data_dir):
                    # Convert to container path
                    rel_path = os.path.relpath(input_path, host_data_dir)
                    container_path = f"/data/{rel_path}"
                    processed_parameters.append(f"-i={container_path}")
                # Otherwise, assume it's already relative to container's /data
                else:
                    # If no /data prefix, add it
                    if not input_path.startswith("/"):
                        container_path = f"/data/{input_path}"
                    else:
                        container_path = input_path
                    processed_parameters.append(f"-i={container_path}")
            else:
                processed_parameters.append(param)

        # Process input_file
        if not os.path.isabs(input_file):
            input_file = os.path.join(container_data_dir, input_file)
        elif input_file.startswith(host_data_dir):
            # Convert host path to container path
            rel_path = os.path.relpath(input_file, host_data_dir)
            input_file = os.path.join(container_data_dir, rel_path)
        elif not input_file.startswith("/data/"):
            # Try to make it a valid container path
            input_file = f"/data/{os.path.basename(input_file)}"

        # Build command with parameters
        command = [input_file]
        command.extend(processed_parameters)

        # Add output file parameter if not specified
        if not any(param.startswith("-o=") for param in processed_parameters):
            relative_output = os.path.join("output", f"output_{job_id}.las")
            command.append(f"-o=/data/{relative_output}")

        logger.info(f"Running container with command: {command}")

        # Run the container
        container = client.containers.run(
            image=f"{image_name}:{image_tag}",
            command=command,
            volumes={host_data_dir: {"bind": container_data_dir, "mode": "rw"}},
            detach=True,
            remove=True,
        )

        # Wait for the container to finish and collect logs
        result = container.wait()
        logs = container.logs().decode("utf-8")

        # Check if the process was successful
        if result["StatusCode"] != 0:
            logger.error(f"Container exited with status {result['StatusCode']}: {logs}")
            self.update_state(
                state="FAILURE",
                meta={
                    "error": f"Processing failed with exit code {result['StatusCode']}",
                    "logs": logs,
                },
            )
            # Retry the task if appropriate
            if self.request.retries < self.max_retries:
                logger.info(f"Retrying task, attempt {self.request.retries + 1}")
                raise self.retry(countdown=60)
            return {
                "status": "error",
                "job_id": job_id,
                "exit_code": result["StatusCode"],
                "logs": logs,
            }

        # Return success result
        return {
            "status": "success",
            "job_id": job_id,
            "output_file": f"output_{job_id}.las",
            "logs": logs,
        }
    except docker.errors.ImageNotFound:
        logger.error(f"Docker image {image_name}:{image_tag} not found")
        self.update_state(state="FAILURE", meta={"error": "Docker image not found"})
        return {"status": "error", "job_id": job_id, "error": "Docker image not found"}
    except docker.errors.APIError as e:
        error_message = str(e)
        logger.error(f"Docker API error: {error_message}")
        # Provide more helpful error messages for common issues
        if "denied: denied" in error_message:
            error_details = "Authentication error with Docker registry. For public images, no authentication should be needed."
        elif "connection refused" in error_message.lower():
            error_details = (
                "Connection to Docker daemon refused. Check if Docker is running."
            )
        else:
            error_details = error_message
        self.update_state(state="FAILURE", meta={"error": error_details})
        return {"status": "error", "job_id": job_id, "error": error_details}
    except Exception as e:
        logger.exception(f"Unexpected error processing job {job_id}")
        self.update_state(state="FAILURE", meta={"error": str(e)})
        return {"status": "error", "job_id": job_id, "error": str(e)}


# Make this file runnable for development
if __name__ == "__main__":
    celery_app.start()
