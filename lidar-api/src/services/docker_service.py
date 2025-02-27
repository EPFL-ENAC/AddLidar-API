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

        # Log the ROOT_VOLUME value being used
        logger.info(f"Using ROOT_VOLUME: {settings.ROOT_VOLUME}")
        
        # Always mount to /data inside the container
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
        
        # Always use /data path inside the container regardless of the host path
        container_output_path = f"/data/{output_file_path}"

        # Process input paths to ensure they use the container path (/data)
        processed_cli_args = []
        for arg in cli_args:
            # Check if this is an input file argument (-i=something)
            if arg.startswith("-i="):
                input_path = arg[3:]  # Extract the path part after "-i="
                
                # If the path already starts with /data, use it as is
                if input_path.startswith("/data/"):
                    processed_cli_args.append(arg)
                # If it's an absolute path from the host ROOT_VOLUME
                elif os.path.isabs(input_path) and input_path.startswith(settings.ROOT_VOLUME):
                    # Convert to container path
                    rel_path = os.path.relpath(input_path, settings.ROOT_VOLUME)
                    container_path = f"/data/{rel_path}"
                    processed_cli_args.append(f"-i={container_path}")
                # Otherwise, assume it's already relative to container's /data
                else:
                    # If no /data prefix, add it
                    if not input_path.startswith("/"):
                        container_path = f"/data/{input_path}"
                    else:
                        container_path = input_path
                    processed_cli_args.append(f"-i={container_path}")
            else:
                processed_cli_args.append(arg)
                
        # Add the output file argument to CLI args
        output_args = [f"-o={container_output_path}"]
        full_cli_args = processed_cli_args + output_args
        
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
