"""
Celery worker for processing LiDAR point cloud data asynchronously
"""
from celery import Celery
import docker
import os
import logging
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
    backend=os.environ.get("CELERY_RESULT_BACKEND", "redis://redis:6379/0")
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

@celery_app.task(bind=True, name="process_lidar_data", max_retries=3)
def process_lidar_data(
    self,
    job_id: str,
    input_file: str,
    parameters: Optional[List[str]] = None,
    image_name: str = os.environ.get("IMAGE_NAME", "ghcr.io/epfl-enac/lidardatamanager"),
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
        
        # Set up volume paths
        host_data_dir = os.environ.get("HOST_DATA_DIR", "/app/data")
        container_data_dir = "/data"
        output_path = os.path.join(host_data_dir, f"output_{job_id}.las")
        
        # Default parameters if none provided
        if parameters is None:
            parameters = []
            
        # Ensure input_file is properly referenced
        if not os.path.isabs(input_file):
            input_file = os.path.join(container_data_dir, input_file)
            
        # Build command with parameters
        command = [input_file]
        command.extend(parameters)
        
        # Add output file parameter if not specified
        if not any(param.startswith("-o=") for param in parameters):
            relative_output = os.path.join("output", f"output_{job_id}.las")
            command.append(f"-o={relative_output}")
            
        logger.info(f"Running container with command: {command}")
            
        # Run the container
        container = client.containers.run(
            image=f"{image_name}:{image_tag}",
            command=command,
            volumes={
                host_data_dir: {
                    "bind": container_data_dir,
                    "mode": "rw"
                }
            },
            detach=True,
            remove=True,
        )
        
        # Wait for the container to finish and collect logs
        result = container.wait()
        logs = container.logs().decode("utf-8")
        
        # Check if the process was successful
        if result["StatusCode"] != 0:
            logger.error(f"Container exited with status {result['StatusCode']}: {logs}")
            self.update_state(state="FAILURE", meta={
                "error": f"Processing failed with exit code {result['StatusCode']}",
                "logs": logs
            })
            
            # Retry the task if appropriate
            if self.request.retries < self.max_retries:
                logger.info(f"Retrying task, attempt {self.request.retries + 1}")
                raise self.retry(countdown=60)
            
            return {
                "status": "error",
                "job_id": job_id,
                "exit_code": result["StatusCode"],
                "logs": logs
            }
        
        # Return success result
        return {
            "status": "success",
            "job_id": job_id,
            "output_file": f"output_{job_id}.las",
            "logs": logs
        }
        
    except docker.errors.ImageNotFound:
        logger.error(f"Docker image {image_name}:{image_tag} not found")
        self.update_state(state="FAILURE", meta={"error": "Docker image not found"})
        return {"status": "error", "job_id": job_id, "error": "Docker image not found"}
        
    except docker.errors.APIError as e:
        logger.error(f"Docker API error: {str(e)}")
        self.update_state(state="FAILURE", meta={"error": str(e)})
        return {"status": "error", "job_id": job_id, "error": str(e)}
        
    except Exception as e:
        logger.exception(f"Unexpected error processing job {job_id}")
        self.update_state(state="FAILURE", meta={"error": str(e)})
        return {"status": "error", "job_id": job_id, "error": str(e)}

# Make this file runnable for development
if __name__ == "__main__":
    celery_app.start()