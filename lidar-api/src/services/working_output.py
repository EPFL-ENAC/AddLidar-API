from src.config.settings import settings


# def run_lidar_cli(command_args: list[str]) -> tuple[bytes, int]:
#     import subprocess

#     try:
#         result = subprocess.run(
#             command_args, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE
#         )
#         if result.returncode == 0:
#             return result.stdout, 0
#         # TODO: remove when addlidarmanager is fixed
#         if result.returncode == 1:
#             return result.stdout, 0
#         return result.stderr, result.returncode
#     except Exception as e:
#         return str(e).encode("utf-8"), 1


def process_point_cloud(cli_args: list[str]) -> tuple[bytes, int]:
    import docker
    import logging

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
        print("f)
        print(f"Running container with image: {settings.IMAGE_NAME + ":" + settings.IMAGE_TAG}", flush=True)
        container = client.containers.create(
            settings.IMAGE_NAME + ":" + settings.IMAGE_TAG,
            command=cli_args,
            volumes={
                f"{settings.ROOT_VOLUME}": {"bind": "/data", "mode": "ro"},
            },
        )
        container.start()
        result = container.wait()
        output = container.logs(stdout=True, stderr=False)
        # If you need stderr separately
        stderr = container.logs(stdout=False, stderr=True)
        container.remove()

        # result will be a dict with 'StatusCode' and potentially 'Error'
        returncode = result['StatusCode']
        if returncode == 0:
            return output, 0
        # # TODO: remove when addlidarmanager is fixed
        if returncode == 1:
            return output, 0
        # return stderr, returncode
        return output, returncode
    except docker.errors.ImageNotFound:
        logger.error(f"Docker image {settings.IMAGE_NAME}:{settings.IMAGE_TAG} not found")
        return f"Docker image {settings.IMAGE_NAME}:{settings.IMAGE_TAG} not found".encode("utf-8"), 1
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