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


def build_docker_command(
    job_id: str,
    input_file: str,
    parameters: Optional[List[str]],
    host_data_dir: str,
    container_data_dir: str,
) -> List[str]:
    """
    Build the Docker command to run in the container by processing input file and parameters.

    Args:
        job_id: Unique job identifier
        input_file: Path to the input file
        parameters: Additional CLI parameters
        host_data_dir: Host directory for data volumes
        container_data_dir: Container directory for data volumes

    Returns:
        List of command arguments
    """
    # Process parameters
    if parameters is None:
        parameters = []
    processed_parameters: List[str] = []
    for param in parameters:
        if param.startswith("-i="):
            input_path = param[3:]
            if input_path.startswith("/data/"):
                processed_parameters.append(param)
            elif os.path.isabs(input_path) and input_path.startswith(host_data_dir):
                rel_path = os.path.relpath(input_path, host_data_dir)
                container_path = f"/data/{rel_path}"
                processed_parameters.append(f"-i={container_path}")
            else:
                # Ensure no leading slash
                container_path = f"/data/{input_path.lstrip('/')}"
                processed_parameters.append(f"-i={container_path}")
        else:
            processed_parameters.append(param)

    # Process input_file
    if not os.path.isabs(input_file):
        container_input_file = os.path.join(container_data_dir, input_file)
    elif input_file.startswith(host_data_dir):
        rel_path = os.path.relpath(input_file, host_data_dir)
        container_input_file = os.path.join(container_data_dir, rel_path)
    elif not input_file.startswith("/data/"):
        container_input_file = f"/data/{os.path.basename(input_file)}"
    else:
        container_input_file = input_file

    command = [container_input_file] + processed_parameters

    if not any(param.startswith("-o=") for param in processed_parameters):
        relative_output = os.path.join("output", f"output_{job_id}.las")
        command.append(f"-o=/data/{relative_output}")

    return command


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

        # Set up volume paths
        host_data_dir = os.environ.get(
            "ROOT_VOLUME", os.environ.get("HOST_DATA_DIR", "/app/data")
        )
        container_data_dir = "/data"

        logger.info(f"Using host data directory: {host_data_dir}")

        # Ensure output directory exists
        output_dir = os.path.join(host_data_dir, "output")
        os.makedirs(output_dir, exist_ok=True)

        # Build Docker command using helper function
        command = build_docker_command(
            job_id, input_file, parameters, host_data_dir, container_data_dir
        )

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
            if self.request.retries < self.max_retries:
                logger.info(f"Retrying task, attempt {self.request.retries + 1}")
                raise self.retry(countdown=60)
            return {
                "status": "error",
                "job_id": job_id,
                "exit_code": result["StatusCode"],
                "logs": logs,
            }

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
