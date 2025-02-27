from src.config.settings import settings
import docker
import logging


def process_point_cloud(cli_args: list[str]) -> tuple[bytes, int]:

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
        print(f"Running container with command: {cli_args}", flush=True)
        print(f"Running container with image: {settings.IMAGE_NAME}", flush=True)
        container = client.containers.create(
            settings.IMAGE_NAME + ":" + settings.IMAGE_TAG,
            command=cli_args,
            volumes={
                f"{settings.ROOT_VOLUME}": {"bind": "/data", "mode": "ro"},
            },
            # network="enac-cd-app_default",
        )
        container.start()
        result = container.wait()
        output = container.logs(stdout=True, stderr=False)
        # If you need stderr separately
        stderr = container.logs(stdout=False, stderr=True)
        container.remove()

        # result will be a dict with 'StatusCode' and potentially 'Error'
        returncode = result["StatusCode"]
        if returncode == 0:
            return output, 0
        else:
            return stderr, returncode
    except Exception as e:
        return str(e).encode("utf-8"), 1
    except docker.errors.APIError as e:
        # Improved error handling for Docker API errors
        error_msg = f"Docker API error: {str(e)}"
        logger.error(error_msg)
        return error_msg.encode("utf-8"), 1
    except Exception as e:
        # General error handling
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        return error_msg.encode("utf-8"), 1
    finally:
        client.close()
        logger.info("Docker client closed")
        return output, 0
