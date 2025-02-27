from kubernetes import client, config, watch
import time
import os
import uuid
import logging
from typing import Tuple, Optional, List, Dict, Any

logger = logging.getLogger(__name__)

# Default settings - would typically be loaded from environment variables or config files
DEFAULT_SETTINGS: Dict[str, Any] = {
    "IMAGE_NAME": "ghcr.io/epfl-enac/lidardatamanager",
    "IMAGE_TAG": "latest",
    "ROOT_VOLUME": "",  # Will be set based on PVC
    "NAMESPACE": "default",
    "MOUNT_PATH": "/data",
    "PVC_NAME": "lidar-data-pvc",  # Default to our created PVC
    "JOB_TIMEOUT": 300,  # Timeout in seconds for job completion
}


def get_settings() -> Dict[str, Any]:
    """
    Get settings from environment variables or use defaults.

    Returns:
        Dict[str, Any]: Dictionary of configuration settings
    """
    settings = DEFAULT_SETTINGS.copy()

    # Override with environment variables if available
    for key in settings:
        env_val = os.environ.get(key)
        if env_val:
            if key in ["JOB_TIMEOUT", "PORT"]:
                settings[key] = int(env_val)
            else:
                settings[key] = env_val

    # Set ROOT_VOLUME based on PVC
    if not settings["ROOT_VOLUME"]:
        settings["ROOT_VOLUME"] = f"pvc:{settings['PVC_NAME']}"

    return settings


def process_point_cloud(cli_args: List[str]) -> Tuple[bytes, int, Optional[str]]:
    """
    Process point cloud data using a Kubernetes job.

    Args:
        cli_args: CLI arguments to pass to the container

    Returns:
        Tuple of (output data, exit code, output file path or None)
    """
    settings = get_settings()

    try:
        # Load Kubernetes config
        try:
            config.load_kube_config()
            logger.info("Using kubeconfig for authentication")
        except Exception as e:
            logger.info(f"Could not load kubeconfig: {str(e)}")
            config.load_incluster_config()
            logger.info("Using in-cluster configuration")

        # Generate a unique job name
        job_name = f"lidar-job-{uuid.uuid4().hex[:10]}"

        # Generate a unique filename for output
        unique_filename = f"output_{uuid.uuid4().hex}.bin"
        output_file_path = os.path.join("output", unique_filename)
        container_output_path = f"{settings['MOUNT_PATH']}/{output_file_path}"

        # Add the output file argument to CLI args
        output_args = [f"-o={container_output_path}"]
        full_cli_args = cli_args + output_args

        # The container image to use
        container_image = f"{settings['IMAGE_NAME']}:{settings['IMAGE_TAG']}"

        logger.info(f"Creating job {job_name} with command: {full_cli_args}")
        logger.info(f"Using container image: {container_image}")

        # Create API clients
        batch_v1 = client.BatchV1Api()
        core_v1 = client.CoreV1Api()

        # Define volume and volume mounts for PVC
        volumes = [
            client.V1Volume(
                name="data-volume",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=settings["PVC_NAME"]
                ),
            )
        ]
        volume_mounts = [
            client.V1VolumeMount(name="data-volume", mount_path=settings["MOUNT_PATH"])
        ]
        logger.info(f"Using PVC: {settings['PVC_NAME']}")

        # Define job container
        container = client.V1Container(
            name="lidar-container",
            image=container_image,
            args=full_cli_args,
            volume_mounts=volume_mounts,
        )

        # Define job
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                name=job_name, namespace=settings["NAMESPACE"]
            ),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    spec=client.V1PodSpec(
                        containers=[container], volumes=volumes, restart_policy="Never"
                    )
                ),
                backoff_limit=0,  # No retries
                ttl_seconds_after_finished=3600,  # Auto-delete job after 1 hour
            ),
        )

        # Create the job
        batch_v1.create_namespaced_job(namespace=settings["NAMESPACE"], body=job)

        # Wait for job completion
        job_status = {"succeeded": False, "failed": False}
        w = watch.Watch()

        logger.info(
            f"Waiting for job {job_name} to complete (timeout: {settings['JOB_TIMEOUT']}s)..."
        )
        for event in w.stream(
            batch_v1.list_namespaced_job,
            namespace=settings["NAMESPACE"],
            timeout_seconds=settings["JOB_TIMEOUT"],
        ):
            if event["object"].metadata.name == job_name:
                job_obj = event["object"]

                # Check if job has completed
                if job_obj.status.succeeded:
                    job_status["succeeded"] = True
                    logger.info(f"Job {job_name} succeeded")
                    w.stop()
                    break
                elif job_obj.status.failed:
                    job_status["failed"] = True
                    logger.error(f"Job {job_name} failed")
                    w.stop()
                    break

        # Get the pod associated with the job
        label_selector = f"job-name={job_name}"
        pods = core_v1.list_namespaced_pod(
            namespace=settings["NAMESPACE"], label_selector=label_selector
        )

        if not pods.items:
            logger.error(f"No pods found for job {job_name}")
            return b"No pods found for this job", 1, None

        pod_name = pods.items[0].metadata.name

        # Get the logs
        logs = core_v1.read_namespaced_pod_log(
            name=pod_name, namespace=settings["NAMESPACE"]
        )

        # Convert logs to bytes
        logs_bytes = logs.encode("utf-8")

        # Clean up job
        try:
            delete_options = client.V1DeleteOptions(propagation_policy="Background")
            batch_v1.delete_namespaced_job(
                name=job_name, namespace=settings["NAMESPACE"], body=delete_options
            )
            logger.info(f"Deleted job {job_name}")
        except Exception as e:
            logger.warning(f"Failed to delete job {job_name}: {str(e)}")

        # Return appropriate results based on job status
        if job_status["succeeded"]:
            return logs_bytes, 0, output_file_path
        else:
            return logs_bytes, 1, None

    except Exception as e:
        error_msg = f"Kubernetes job error: {str(e)}"
        logger.error(error_msg)
        return error_msg.encode("utf-8"), 1, None


# Example usage to demonstrate the functionality
def example_usage() -> None:
    """
    Example function showing how to use the process_point_cloud function.
    Note: When using PVC, paths should be relative to the mount point
    """
    # Example CLI arguments for LidarDataManager - each argument must be a separate list item
    cli_args = [
        "/data/LiDAR/0001_Mission_Root/02_LAS_PCD/all_grouped_high_veg_10th_point.las",  # Path relative to mount point
        "--format=pcd-ascii",
    ]

    output, exit_code, output_file_path = process_point_cloud(cli_args)

    print(
        f"Job {'succeeded' if exit_code == 0 else 'failed'} with exit code: {exit_code}"
    )
    print(f"Output file path: {output_file_path}")
    print("First 200 bytes of output:")
    print(output[:200])


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    print("Testing LidarDataManager Kubernetes job runner")
    example_usage()
