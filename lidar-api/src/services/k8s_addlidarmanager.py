from kubernetes import client, config
from kubernetes.watch import Watch
import uuid
import logging
import asyncio
from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List, Dict, Any
from src.config.settings import settings

# from src.services.job_status import job_status_manager


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

AUTHORIZED_STATUSES = ["Complete", "SuccessCriteriaMet", "Failed", "FailureTarget"]


class JobStatus(BaseModel):
    """Model representing the status of a job."""

    job_name: Optional[str]
    status: Optional[str]
    message: Optional[str]
    created_at: Optional[datetime] = None
    total_time: Optional[float] = None  # in seconds
    timestamp: Optional[datetime] = None
    cli_args: Optional[List[str]] = None
    output_path: Optional[str] = None
    logs: Optional[str] = None  # Changed from bytes to str

    class Config:
        json_encoders = {
            # Custom JSON encoder for datetime
            datetime: lambda dt: dt.isoformat() if dt else None
        }


# Store job statuses in memory
job_statuses: Dict[str, Dict[str, Any]] = {}


def update_job_statuses(
    job_name: str, job_status: JobStatus, loop: asyncio.AbstractEventLoop
) -> None:
    """
    Update the status of a job in the job_statuses dictionary.
    Only updates fields that are provided in the new status, preserving existing values.

    Args:
        job_name: Name of the job to update
        job_status: Status information for the job
        loop: Event loop to schedule async tasks on
    """
    # Get current job status if it exists
    current_status = job_statuses.get(job_name, {})

    # Convert new status to dict
    new_status = job_status.dict(exclude_unset=True, exclude_none=True)

    # Merge statuses, preserving existing values for fields not in new_status
    merged_status = {**current_status, **new_status}
    merged_status["timestamp"] = datetime.now()

    # Store the merged status
    job_statuses[job_name] = merged_status

    # Use the passed event loop to notify connected WebSocket clients
    if job_name in active_connections:
        # Use JobStatus object directly instead of dict to avoid type issues
        status_object = JobStatus(**merged_status)
        asyncio.run_coroutine_threadsafe(notify_websocket(status_object), loop)

    logger.debug(f"Updated job status for {job_name}: {merged_status}")


def get_settings() -> Dict[str, Any]:
    """
    Get settings from environment variables or use defaults.

    Returns:
        Dict[str, Any]: Dictionary of configuration settings
    """
    return settings.dict()


def get_pod_info(pod_name: str) -> str:
    """
    Get information about a pod.

    Args:
        pod_name: Name of the pod to get information for

    Returns:
        str: Information about the pod
    """
    core_v1 = client.CoreV1Api()
    settings_dict = get_settings()
    pod = core_v1.read_namespaced_pod(
        name=pod_name, namespace=settings_dict["NAMESPACE"]
    )
    pod_info = f"Pod phase: {pod.status.phase}\n"
    if pod.status.container_statuses:
        for container in pod.status.container_statuses:
            pod_info += f"Container {container.name} ready: {container.ready}\n"
            if container.state.waiting:
                pod_info += f"  Waiting: {container.state.waiting.reason} - {container.state.waiting.message}\n"
            if container.state.terminated:
                pod_info += (
                    f"  Terminated: {container.state.terminated.reason} - "
                    f"Exit code: {container.state.terminated.exit_code} - "
                    f"Message: {container.state.terminated.message}\n"
                )
    return pod_info


def get_log_job_status(job_name: str) -> str:
    # Get the pod associated with the job
    settings_dict = get_settings()

    label_selector = f"job-name={job_name}"
    core_v1 = client.CoreV1Api()
    pods = core_v1.list_namespaced_pod(
        namespace=settings_dict["NAMESPACE"], label_selector=label_selector
    )

    if not pods.items:
        logger.error(f"No pods found for job {job_name}")
        return b"No pods found for this job", 1, None

    pod_name = pods.items[0].metadata.name
    logger.info(f"Pod name: {pod_name}")

    try:
        # Get the logs
        logs = core_v1.read_namespaced_pod_log(
            name=pod_name, namespace=settings_dict["NAMESPACE"]
        )

        # if not logs or logs == "\n":
        #     # If logs are empty, try to get pod status information
        #     logs = get_pod_info(pod_name)
        # else:
        #     return logs
        if not logs or logs == "\n":
            logs = "No logs available\n"
        return logs + "\n" + get_pod_info(pod_name)

    except Exception as e:
        logger.error(f"Error getting logs for job {job_name}: {str(e)}")
        return f"Error retrieving logs: {str(e)}"


def watch_job_status_thread(
    job_name: str, namespace: str, loop: asyncio.AbstractEventLoop
) -> None:
    """
    Watches a Kubernetes Job in a separate thread and sends status updates via event loop.

    Args:
        job_name: Name of the job to watch
        namespace: Kubernetes namespace where the job exists
        loop: Event loop to schedule async tasks on
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

                # Then use methods
                # status = job_status_manager.get_detailed_job_status(job_name)
                # simple_status = job_status_manager.interpret_job_status(status)
                # logger.info(f"Simple status: {simple_status}")
                if job.status.active == 1:
                    # logger.info(f"Job {job_name} is running, updating status, {str(job.status)}")
                    update_job_statuses(
                        job_name,
                        JobStatus(
                            job_name=job_name,
                            status="Running",
                            message="Job is running",
                        ),
                        loop,
                    )
                if conditions:
                    status = conditions[0].type
                    logs = None
                    if status in AUTHORIZED_STATUSES:
                        try:
                            logs = get_log_job_status(job_name)
                        except Exception as log_error:
                            logger.error(
                                f"Error getting logs for job {job_name}: {str(log_error)}"
                            )
                            logs = f"Error retrieving logs: {str(log_error)}"

                    update_job_statuses(
                        job_name,
                        JobStatus(
                            job_name=job_name,
                            status=status,
                            message=f"Job {job_name} {status}",
                            logs=logs if logs else "no logs",
                        ),
                        loop,
                    )
                    if status in AUTHORIZED_STATUSES:
                        delete_k8s_job(job_name, namespace)
                        w.stop()
                        break

    except Exception as e:
        logger.error(f"Error watching job {job_name}: {str(e)}")
        update_job_statuses(
            job_name,
            JobStatus(
                job_name=job_name,
                status="Error",
                message=f"Error watching job: {str(e)}",
            ),
            loop,
        )
    finally:
        # Clean up the watch control entry
        if job_name in watch_control:
            logger.info(f"Cleaning up watch control: {job_name}")
            del watch_control[job_name]


async def notify_websocket(job_status: JobStatus) -> None:
    """
    Send a message to WebSocket client.

    Args:
        job_status: Status information for the job including job_name
                    Can be either a JobStatus object or a dictionary
    """
    try:
        job_name = extract_job_name(job_status)
        if not job_name:
            logger.error(f"Job name is missing in job status: {job_status}")
            return

        if job_name in active_connections:
            connection = active_connections[job_name]
            status_dict = prepare_status_dict(job_status)
            await connection.send_json(status_dict)
            logger.info(
                f"WebSocket notification sent for job {job_name}: {job_status.message}"
            )

            if job_status.status in AUTHORIZED_STATUSES:
                await connection.close()
                logger.info(f"Closed WebSocket for completed job {job_name}")
                del active_connections[job_name]
    except Exception as e:
        handle_notification_error(e, job_status)


def extract_job_name(job_status: JobStatus) -> Optional[str]:
    if not isinstance(job_status, JobStatus):
        raise ValueError("job_status must be a JobStatus object")
    return job_status.job_name


def prepare_status_dict(job_status: JobStatus) -> Dict[str, Any]:
    status_dict = job_status.dict(exclude_unset=True)
    if status_dict.get("timestamp") and isinstance(status_dict["timestamp"], datetime):
        status_dict["timestamp"] = status_dict["timestamp"].isoformat()
    if status_dict.get("created_at") and isinstance(
        status_dict["created_at"], datetime
    ):
        status_dict["created_at"] = status_dict["created_at"].isoformat()
    if "logs" in status_dict and isinstance(status_dict["logs"], bytes):
        status_dict["logs"] = status_dict["logs"].decode("utf-8", errors="replace")
    if status_dict.get("timestamp") and status_dict.get("created_at"):
        timestamp = datetime.fromisoformat(status_dict["timestamp"])
        created_at = datetime.fromisoformat(status_dict["created_at"])
        status_dict["total_time"] = (timestamp - created_at).total_seconds()
    return status_dict


def handle_notification_error(e: Exception, job_status: JobStatus) -> None:
    job_name_str = "unknown"
    try:
        job_name_str = (
            job_status.job_name
            if isinstance(job_status, JobStatus)
            else job_status.get("job_name", "unknown")
        )
        logger.error(f"Error notifying WebSocket for job {job_name_str}: {str(e)}")
        if job_name_str in active_connections:
            del active_connections[job_name_str]
    except Exception as nested_e:
        logger.error(
            f"Critical error in notify_websocket error handler: {str(nested_e)}"
        )


def stop_watching_job(job_name: str) -> None:
    """
    Stops watching a job.

    Args:
        job_name: Name of the job to stop watching
    """
    if job_name in watch_control:
        watch_control[job_name] = False
        logger.info(f"Stopping job watcher for job {job_name}")
    else:
        logger.warning(f"No watch control found for job {job_name}")


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

    # Capture the current event loop to pass to the thread
    loop = asyncio.get_event_loop()
    try:
        # Start new watch thread
        thread = threading.Thread(
            target=watch_job_status_thread,
            args=(job_name, namespace, loop),
            daemon=True,
        )
        thread.start()
        logger.info(f"Started job watcher thread for job {job_name}")
    except RuntimeError as e:
        update_job_statuses(
            job_name,
            JobStatus(
                job_name=job_name,
                status="Error",
                message=f"Failed to start job watcher: {str(e)}",
            ),
            loop,
        )


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
        status_dict = job_statuses[job_name]
        # Convert to JobStatus object to ensure type safety
        status_object = JobStatus(**status_dict)
        asyncio.create_task(notify_websocket(status_object))


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


def generate_k8s_addlidarmanager_job(
    job_name: str, unique_filename: str, cli_args: Optional[List[str]]
) -> None:
    """
    Create a Kubernetes job that runs the LidarDataManager container.

    Args:
        job_name: Name of the job to create
        cli_args: CLI arguments to pass to the container

    Returns:
        str: The name of the created job
    """
    settings_dict = get_settings()
    container_output_path = f"{settings_dict['OUTPUT_PATH']}/{unique_filename}"

    # Add the output file argument to CLI args
    output_args = [f"-o={container_output_path}"]
    full_cli_args = cli_args + output_args

    # The container image to use
    container_image = f"{settings_dict['IMAGE_NAME']}:{settings_dict['IMAGE_TAG']}"

    logger.info(f"Creating job {job_name} with command: {full_cli_args}")
    logger.info(f"Using container image: {container_image}")

    # Create API clients
    batch_v1 = client.BatchV1Api()

    # Define volume and volume mounts for PVC
    volumes = [
        client.V1Volume(
            name="data-volume",
            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                claim_name=settings_dict["PVC_NAME"]
            ),
        ),
        client.V1Volume(
            name="data-output-volume",
            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                claim_name=settings_dict["PVC_OUTPUT_NAME"]
            ),
        ),
    ]
    volume_mounts = [
        client.V1VolumeMount(
            name="data-volume",
            mount_path=settings_dict["MOUNT_PATH"],
            sub_path=settings_dict["SUB_PATH"],
        ),
        client.V1VolumeMount(
            name="data-output-volume", mount_path=settings_dict["OUTPUT_PATH"]
        ),
    ]
    logger.info(f"Using PVC: {settings_dict['PVC_NAME']}")
    logger.info(f"Using PVC OUTPOUT: {settings_dict['PVC_OUTPUT_NAME']}")

    # Define job container
    container = client.V1Container(
        name="lidar-container",
        image=container_image,
        args=full_cli_args,
        volume_mounts=volume_mounts,
        resources=client.V1ResourceRequirements(
            requests={
                "cpu": "500m",  # Request 1 CPU cores
                "memory": "128Mi",  # Request 128 MiB memory
            },
            limits={
                "cpu": "1000m",  # limits 1 CPU cores max
                "memory": "256Mi",  # limits 512 MiB memory
            },
        ),
    )
    # Create labels based on environment
    annotations = {}
    app_name = "addlidar-api"
    environment = settings_dict["ENVIRONMENT"]

    if environment == "production":
        app_name = "addlidar-api-prod"
    else:  # development or any other environment
        app_name = "addlidar-api-dev"

    annotations["argocd.argoproj.io/instance"] = app_name
    # Define job
    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(
            name=job_name,
            namespace=settings_dict["NAMESPACE"],
            labels={"app": app_name},
            annotations=annotations,
        ),
        spec=client.V1JobSpec(
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"app": app_name}),
                spec=client.V1PodSpec(
                    containers=[container], volumes=volumes, restart_policy="Never"
                ),
            ),
            backoff_limit=3,  # No retries
            ttl_seconds_after_finished=7200,  # Auto-delete job after 2 hour
        ),
    )

    # Create the job
    batch_v1.create_namespaced_job(namespace=settings_dict["NAMESPACE"], body=job)
    logger.info(f"Created job {job_name}")
    return job_name


def create_k8s_job(job_name: str, cli_args: Optional[List[str]]) -> None:
    """
    Create a Kubernetes job that runs a simple hello world command.

    Args:
        job_name: Name of the job to create

    Returns:
        Tuple[str, int]: The output (stdout or stderr) and exit code
    """
    # settings = get_settings()

    try:
        # Generate a unique filename for output
        unique_filename = f"output_{uuid.uuid4().hex}.bin"
        generate_k8s_addlidarmanager_job(job_name, unique_filename, cli_args)
        update_job_statuses(
            job_name,
            JobStatus(
                job_name=job_name,
                status="Created",
                created_at=datetime.now(),
                message="Job is created",
                output_path=unique_filename,
                cli_args=cli_args,
            ),
            asyncio.get_event_loop(),
        )
        logger.info(
            f"Created job {job_name}, output will be saved to {unique_filename}, cli_args: {cli_args}"
        )
        logger.info(f"Created job {job_name}")
        return job_name
    except Exception as e:
        error_msg = f"Failed to create or run job {job_name}: {str(e)}"
        logger.error(error_msg)
        return error_msg, 1
