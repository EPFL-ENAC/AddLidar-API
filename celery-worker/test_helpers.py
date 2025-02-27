"""
Test helpers for Celery tasks
"""
import docker
import os
from typing import Dict, List, Any, Optional


def lidar_processing_helper(
    job_id: str,
    input_file: str,
    parameters: Optional[List[str]] = None,
    image_name: str = "ghcr.io/epfl-enac/lidardatamanager",
    image_tag: str = "latest"
) -> Dict[str, Any]:
    """
    Core logic for processing LiDAR data - extracted for testability
    
    Args:
        job_id: Unique identifier for the job
        input_file: Path to the input LiDAR file
        parameters: CLI parameters to pass to the processing container
        image_name: Docker image name for LiDAR processing
        image_tag: Docker image tag
    
    Returns:
        Dictionary containing job results and metadata
    """
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
            # Process failed
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
        return {"status": "error", "job_id": job_id, "error": "Docker image not found"}
        
    except docker.errors.APIError as e:
        return {"status": "error", "job_id": job_id, "error": str(e)}
        
    except Exception as e:
        return {"status": "error", "job_id": job_id, "error": str(e)}