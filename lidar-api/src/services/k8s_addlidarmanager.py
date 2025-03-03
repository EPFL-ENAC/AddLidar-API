from kubernetes import client, config, watch
import uuid
import logging
import asyncio
from typing import Tuple, Optional, List, Dict, Any
from src.config.settings import settings

from kubernetes.watch import Watch
import threading

logger = logging.getLogger(__name__)

# Load Kubernetes config
try:
    config.load_kube_config()
    logger.info("Using kubeconfig for authentication")
except Exception as e:
    logger.info(f"Could not load kubeconfig: {str(e)}")
    config.load_incluster_config()
    logger.info("Using in-cluster configuration")
batch_v1 = client.BatchV1Api()

# Store WebSocket connections
active_connections: Dict[str, Any] = {}

# Dictionary to control running watch loops
watch_control: Dict[str, bool] = {}

def get_settings() -> Dict[str, Any]:
    """
    Get settings from environment variables or use defaults.

    Returns:
        Dict[str, Any]: Dictionary of configuration settings
    """
    return settings.dict()


job_statuses: Dict[str, Dict[str, Any]] = {}  # Store job statuses in memory


def watch_job_status_thread(job_name: str, namespace: str = "default") -> None:
    """
    Watches a Kubernetes Job in a separate thread and sends status updates via event loop.
    
    Args:
        job_name: Name of the job to watch
        namespace: Kubernetes namespace where the job exists
    """
    try:
        batch_v1 = client.BatchV1Api()
        w = Watch()
        watch_control[job_name] = True
        
        logger.info(f"Started watching job {job_name} in namespace {namespace}")
        
        for event in w.stream(batch_v1.list_namespaced_job, namespace=namespace):
            # Check if we should stop watching
            if not watch_control.get(job_name, True):
                logger.info(f"Stopping watch for job {job_name}")
                w.stop()
                break
                
            job = event["object"]
            if job.metadata.name == job_name:
                conditions = job.status.conditions
                if conditions:
                    job_status = conditions[0].type
                    if job_status == "Complete":
                        message = f"Job {job_name} Completed"
                        job_statuses[job_name] = {"status": "Complete", "message": message}
                        # Schedule the async notification on the event loop
                        if job_name in active_connections:
                            asyncio.run_coroutine_threadsafe(
                                notify_websocket(job_name, message), 
                                asyncio.get_event_loop()
                            )
                        w.stop()
                        break
                    elif job_status == "Failed":
                        message = f"Job {job_name} Failed"
                        job_statuses[job_name] = {"status": "Failed", "message": message}
                        # Schedule the async notification on the event loop
                        if job_name in active_connections:
                            asyncio.run_coroutine_threadsafe(
                                notify_websocket(job_name, message), 
                                asyncio.get_event_loop()
                            )
                        w.stop()
                        break
    except Exception as e:
        logger.error(f"Error watching job {job_name}: {str(e)}")
        # Attempt to notify about error
        job_statuses[job_name] = {"status": "Error", "message": f"Error watching job: {str(e)}"}
        if job_name in active_connections:
            asyncio.run_coroutine_threadsafe(
                notify_websocket(job_name, f"Error: {str(e)}"),
                asyncio.get_event_loop()
            )
    finally:
        # Clean up the watch control entry
        if job_name in watch_control:
            del watch_control[job_name]


async def notify_websocket(job_name: str, message: str) -> None:
    """
    Send a message to WebSocket client.
    
    Args:
        job_name: The job name associated with the WebSocket
        message: Message to send to the client
    """
    try:
        if job_name in active_connections:
            connection = active_connections[job_name]
            # Send as structured JSON instead of plain text
            await connection.send_json({
                "job_name": job_name,
                "status": job_statuses.get(job_name, {}).get("status", "Unknown"),
                "message": message
            })
            logger.info(f"WebSocket notification sent for job {job_name}: {message}")
            
            # Don't automatically close the connection - let the client decide
            # Only close if job is Complete or Failed
            job_status = job_statuses.get(job_name, {}).get("status")
            if job_status in ["Complete", "Failed", "Error"]:
                await connection.close()
                logger.info(f"Closed WebSocket for completed job {job_name}")
                # Clean up the connection reference
                if job_name in active_connections:
                    del active_connections[job_name]
    except Exception as e:
        logger.error(f"Error notifying WebSocket for job {job_name}: {str(e)}")
        # Only cleanup if there was an error
        if job_name in active_connections:
            del active_connections[job_name]


def start_watching_job(job_name: str, namespace: str = "default") -> None:
    """
    Starts watching a job in a separate thread.
    
    Args:
        job_name: Name of the job to watch
        namespace: Kubernetes namespace where the job exists
    """
    # Clean up any existing watch for this job
    if job_name in watch_control:
        watch_control[job_name] = False
    
    # Initialize job status
    job_statuses[job_name] = {"status": "Running", "message": f"Job {job_name} started"}
    logger.info(job_statuses)
    # Start new watch thread
    thread = threading.Thread(
        target=watch_job_status_thread,
        args=(job_name, namespace),
        daemon=True  # Make thread daemon so it doesn't block program exit
    )
    thread.start()
    logger.info(f"Started job watcher thread for job {job_name}")


def register_websocket(job_name: str, websocket) -> None:
    """
    Register a WebSocket connection for a job.
    
    Args:
        job_name: The job name to associate the WebSocket with
        websocket: The WebSocket connection object
    """
    active_connections[job_name] = websocket
    logger.info(f"Registered WebSocket for job {job_name}")
    
    # If we already have status for this job, send it immediately
    if job_name in job_statuses:
        status = job_statuses[job_name]
        asyncio.create_task(notify_websocket(job_name, status["message"]))


def delete_k8s_job(job_name: str, namespace: str) -> bool:
    """
    Delete a Kubernetes job.

    Args:
        job_name: Name of the job to delete
        namespace: Kubernetes namespace where the job exists

    Returns:
        bool: True if deletion was successful, False otherwise
    """
    try:
        batch_v1 = client.BatchV1Api()
        delete_options = client.V1DeleteOptions(propagation_policy="Background")
        batch_v1.delete_namespaced_job(
            name=job_name, namespace=namespace, body=delete_options
        )
        logger.info(f"Deleted job {job_name}")
        return True
    except Exception as e:
        logger.warning(f"Failed to delete job {job_name}: {str(e)}")
        return False


def create_k8s_job(job_name: str) -> None:
    """
    Create a Kubernetes job that runs a simple hello world command.

    Args:
        job_name: Name of the job to create

    Returns:
        Tuple[str, int]: The output (stdout or stderr) and exit code
    """
    settings = get_settings()

    try:

        # Create API clients
        batch_v1 = client.BatchV1Api()
        core_v1 = client.CoreV1Api()

        # Define job container with simple echo command
        container = client.V1Container(
            name="hello-world",
            image="busybox",
            command=["sh", "-c", "echo 'Hello, Kubernetes!' || exit 1"],
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
                        containers=[container], restart_policy="Never"
                    )
                ),
                backoff_limit=0,  # No retries
            ),
        )

        # Create the job
        batch_v1.create_namespaced_job(namespace=settings["NAMESPACE"], body=job)
        logger.info(f"Created job {job_name}")
        # # Wait for job completion
        # w = watch.Watch()
        # job_succeeded = False
        # job_failed = False

        # for event in w.stream(
        #     batch_v1.list_namespaced_job,
        #     namespace=settings["NAMESPACE"],
        #     timeout_seconds=60
        # ):
        #     if event["object"].metadata.name == job_name:
        #         job_obj = event["object"]
        #         if job_obj.status.succeeded:
        #             job_succeeded = True
        #             w.stop()
        #             break
        #         elif job_obj.status.failed:
        #             job_failed = True
        #             w.stop()
        #             break

        # # Get the pod associated with the job
        # label_selector = f"job-name={job_name}"
        # pods = core_v1.list_namespaced_pod(
        #     namespace=settings["NAMESPACE"],
        #     label_selector=label_selector
        # )

        # if not pods.items:
        #     output = "No pods found for this job"
        #     exit_code = 1
        # else:
        #     pod_name = pods.items[0].metadata.name
        #     # Get the logs
        #     output = core_v1.read_namespaced_pod_log(
        #         name=pod_name,
        #         namespace=settings["NAMESPACE"]
        #     )
        #     exit_code = 0 if job_succeeded else 1

        # # Clean up job
        # delete_k8s_job(job_name, settings["NAMESPACE"])

        # return output, exit_code
        return job_name
    except Exception as e:
        error_msg = f"Failed to create or run job {job_name}: {str(e)}"
        logger.error(error_msg)
        return error_msg, 1


def process_point_cloud(cli_args: List[str]) -> Tuple[bytes, int, Optional[str]]:
    """
    Process point cloud data using a Kubernetes job.

    Args:
        cli_args: CLI arguments to pass to the container

    Returns:
        Tuple of (output data, exit code, output file path or None)
    """
    settings = get_settings()
    logger.info(f"Using settings: {settings}")

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
        container_output_path = f"{settings['OUTPUT_PATH']}/{unique_filename}"

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
            ),
            client.V1Volume(
                name="data-output-volume",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=settings["PVC_OUTPUT_NAME"]
                ),
            ),
        ]
        volume_mounts = [
            client.V1VolumeMount(name="data-volume", mount_path=settings["MOUNT_PATH"]),
            client.V1VolumeMount(
                name="data-output-volume", mount_path=settings["OUTPUT_PATH"]
            ),
        ]
        logger.info(f"Using PVC: {settings['PVC_NAME']}")
        logger.info(f"Using PVC OUTPOUT: {settings['PVC_OUTPUT_NAME']}")

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
        delete_k8s_job(job_name, settings["NAMESPACE"])

        # Return appropriate results based on job status
        if job_status["succeeded"]:
            return logs_bytes, 0, unique_filename
        else:
            return logs_bytes, 1, None

    except Exception as e:
        error_msg = f"Kubernetes job error: {str(e)}"
        logger.error(error_msg)
        # Clean up job
        delete_k8s_job(job_name, settings["NAMESPACE"])
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
