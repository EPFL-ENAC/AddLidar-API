from src.config.settings import settings
import docker
import logging
import os
import uuid
from typing import Tuple, Optional, List


def process_point_cloud(cli_args: List[str]) -> Tuple[bytes, int, Optional[str]]:
    """
    Process point cloud data using a Docker container.

    Args:
        cli_args: CLI arguments to pass to the Docker container

    Returns:
        Tuple of (output data, exit code, output file path or None)
    """
    # Configure logging
    logger = logging.getLogger(__name__)

    try:
        client = docker.from_env()

        # No authentication needed for public images
        # Simply pull the image directly
        logger.info(f"Pulling image {settings.IMAGE_NAME}:{settings.IMAGE_TAG}")
        try:
            client.images.pull(settings.IMAGE_NAME, tag=settings.IMAGE_TAG)
        except docker.errors.APIError as e:
            # If pulling fails but the image exists locally, we can still proceed
            logger.warning(f"Failed to pull image: {e}")
            try:
                client.images.get(f"{settings.IMAGE_NAME}:{settings.IMAGE_TAG}")
                logger.info("Using existing local image")
            except docker.errors.ImageNotFound:
                # Re-raise the original error if image doesn't exist locally
                logger.error("Image not available locally")
                raise e

        volumes = {
            f"{settings.ROOT_VOLUME}": {
                "bind": "/data",
                "mode": "rw",
            },  # Using rw mode to allow writing files
        }

        # Create output directory if it doesn't exist
        output_dir = os.path.join(settings.ROOT_VOLUME, "output")
        os.makedirs(output_dir, exist_ok=True)

        # Generate a unique filename
        unique_filename = f"output_{uuid.uuid4().hex}.bin"
        output_file_path = os.path.join("output", unique_filename)
        container_output_path = f"/data/{output_file_path}"

        # Add the output file argument to CLI args
        output_args = [f"-o={container_output_path}"]
        full_cli_args = cli_args + output_args

        logger.info(f"Running container with command: {full_cli_args}")
        logger.info(f"Running container with image: {settings.IMAGE_NAME}")

        container = client.containers.create(
            settings.IMAGE_NAME + ":" + settings.IMAGE_TAG,
            command=full_cli_args,
            volumes=volumes,
        )

        container.start()
        result = container.wait()

        # Get logs for error messages and debugging
        stdout = container.logs(stdout=True, stderr=False)
        stderr = container.logs(stdout=False, stderr=True)
        container.remove()

        # result will be a dict with 'StatusCode' and potentially 'Error'
        returncode = result["StatusCode"]
        if returncode == 0:
            return stdout, 0, output_file_path
        else:
            # In case of error, return stderr
            return stderr, returncode, None

    except docker.errors.APIError as e:
        # Improved error handling for Docker API errors
        error_msg = f"Docker API error: {str(e)}"
        logger.error(error_msg)
        return error_msg.encode("utf-8"), 1, None
    except Exception as e:
        # General error handling
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        return error_msg.encode("utf-8"), 1, None
    finally:
        # Close the Docker client
        client.close()
        logger.info("Docker client closed")
