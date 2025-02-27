from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, Response, FileResponse
from pydantic import ValidationError, BaseModel
import logging
import os
import uuid
from typing import Dict, Optional, Any
from fastapi.encoders import jsonable_encoder
import time

from src.api.models import PointCloudRequest, ProcessPointCloudResponse

# Replace Docker service import with Kubernetes service
from src.services.k8s_addlidarmanager import process_point_cloud
from src.config.settings import settings
from celery.result import AsyncResult
from celery import Celery
from src.services.parse_docker_error import parse_cli_error

router = APIRouter()
logger = logging.getLogger("uvicorn")

# Configure Celery client
celery_app = Celery(
    "lidar_tasks",
    broker=os.environ.get("CELERY_BROKER_URL", "redis://redis:6379/0"),
    backend=os.environ.get("CELERY_RESULT_BACKEND", "redis://redis:6379/0"),
)


@router.get("/process-point-cloud")
async def process_point_cloud_endpoint(
    background_tasks: BackgroundTasks,
    file_path: str,
    remove_attribute: list[str] | None = None,
    remove_all_attributes: bool = False,
    remove_color: bool = False,
    format: str | None = None,
    line: int | None = None,
    returns: int | None = None,
    number: int | None = None,
    density: float | None = None,
    roi: str | None = None,
    outcrs: str | None = None,
    incrs: str | None = None,
) -> Response:
    try:
        # Convert query params to PointCloudRequest model
        request = PointCloudRequest(
            file_path=file_path,
            remove_attribute=remove_attribute,
            remove_all_attributes=remove_all_attributes,
            remove_color=remove_color,
            format=format,
            line=line,
            returns=returns,
            number=number,
            density=density,
            roi=tuple(map(float, roi.split(","))) if roi else None,
            outcrs=outcrs,
            incrs=incrs,
        )

        cli_args = request.to_cli_arguments()
        logger.debug(f"CLI arguments: {cli_args}")

        # Process point cloud using Kubernetes service instead of Docker
        # The Kubernetes service automatically adds the -o parameter
        output, exit_code, output_file_path = process_point_cloud(cli_args=cli_args)

        # If exit code is 0, return the file data
        if exit_code == 0 and output_file_path:
            # Try to determine content type based on format
            content_type = "application/octet-stream"

            # Map format to appropriate file extension and content type
            format_to_extension = {
                "pcd-ascii": (".pcd", "text/plain"),
                "pcd-binary": (".pcd", "application/octet-stream"),
                "lasv14": (".las", "application/octet-stream"),
                "las": (".las", "application/octet-stream"),
                "laz": (".laz", "application/octet-stream"),
                "ply": (".ply", "application/octet-stream"),
                "ply-ascii": (".ply", "text/plain"),
                "ply-binary": (".ply", "application/octet-stream"),
                "xyz": (".xyz", "text/plain"),
                "txt": (".txt", "text/plain"),
                "csv": (".csv", "text/csv"),
            }

            # Default extension and content type
            extension = ".bin"
            content_type = "application/octet-stream"

            # If format is specified, get the appropriate extension and content type
            if format and format.lower() in format_to_extension:
                extension, content_type = format_to_extension[format.lower()]

            # Get the full path to the output file
            full_output_path = os.path.join(settings.DEFAULT_ROOT, output_file_path)
            print(output_file_path)
            # Create a filename with the appropriate extension
            original_filename = os.path.basename(output_file_path)
            base_filename = os.path.splitext(original_filename)[0]
            file_name = f"{base_filename}{extension}"

            # Log the file name we're serving
            logger.debug(f"Serving file {file_name} with content type {content_type}")

            # Add cleanup task to delete the output file after response is sent
            def remove_output_file(file_path: str) -> None:
                try:
                    if os.path.exists(file_path):
                        os.unlink(file_path)
                        logger.info(f"Deleted temporary output file: {file_path}")
                except Exception as e:
                    logger.error(f"Error deleting output file {file_path}: {str(e)}")

            background_tasks.add_task(remove_output_file, full_output_path)

            return FileResponse(
                path=full_output_path, media_type=content_type, filename=file_name
            )

        # If there was an error, return JSON response
        error_message = output.decode("utf-8", errors="replace")
        message = parse_cli_error(error_message)
        return JSONResponse(
            status_code=400,
            content=ProcessPointCloudResponse(
                status="error",
                error_type="cli_error",
                error_details={
                    "message": message,
                    "cli_args": cli_args,
                    "exit_code": exit_code,
                },
                output="",
            ).model_dump(),
        )

    except ValidationError as e:
        logger.error(f"Validation error: {str(e)}")
        return JSONResponse(
            status_code=400,
            content=ProcessPointCloudResponse(
                status="error",
                output=str(e),
                error_type="validation_error",
                error_details={"errors": jsonable_encoder(e.errors())},
            ).model_dump(),
        )
    except ValueError as e:
        logger.error(f"Value error: {str(e)}")
        return JSONResponse(
            status_code=400,
            content=ProcessPointCloudResponse(
                status="error",
                output=str(e),
                error_type="value_error",
                error_details={"message": str(e)},
            ).model_dump(),
        )
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return JSONResponse(
            status_code=500,
            content=ProcessPointCloudResponse(
                status="error",
                output=str(e),
                error_type="internal_error",
                error_details={"message": "An unexpected error occurred"},
            ).model_dump(),
        )


# Storage for job metadata (in production, use a database)
JOBS_STORE: Dict[str, Dict[str, Any]] = {}
OUTPUT_DIR = os.path.join(settings.DEFAULT_ROOT, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


class ProcessRequest(BaseModel):
    input_file: str
    parameters: Optional[Dict[str, Any]] = {}


class JobResponse(BaseModel):
    job_id: str
    status: str
    task_id: Optional[str] = None
    result: Optional[Dict[str, Any]] = None


@router.get("/jobs", response_model=JobResponse)
async def create_processing_job(
    file_path: str,
    remove_attribute: list[str] | None = None,
    remove_all_attributes: bool = False,
    remove_color: bool = False,
    format: str | None = None,
    line: int | None = None,
    returns: int | None = None,
    number: int | None = None,
    density: float | None = None,
    roi: str | None = None,
    outcrs: str | None = None,
    incrs: str | None = None,
):
    """Start an asynchronous processing job using Celery"""
    # Generate a unique job ID
    job_id = str(uuid.uuid4())

    # Convert query params to PointCloudRequest model
    request = PointCloudRequest(
        file_path=file_path,
        remove_attribute=remove_attribute,
        remove_all_attributes=remove_all_attributes,
        remove_color=remove_color,
        format=format,
        line=line,
        returns=returns,
        number=number,
        density=density,
        roi=tuple(map(float, roi.split(","))) if roi else None,
        outcrs=outcrs,
        incrs=incrs,
    )

    cli_args = request.to_cli_arguments()
    logger.debug(f"CLI arguments: {cli_args}")

    # Submit the job to Celery
    task = celery_app.send_task(
        "process_lidar_data",
        args=[job_id, request.file_path],
        kwargs={"parameters": request.to_cli_arguments()},
    )

    # Store job metadata
    JOBS_STORE[job_id] = {
        "task_id": task.id,
        "status": "PENDING",
        "input_file": file_path,
        "parameters": cli_args,
        "created_at": time.time(),
        "output_file": None,
    }

    return JobResponse(job_id=job_id, status="PENDING", task_id=task.id)


@router.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job_status(job_id: str):
    """Get the status of an asynchronous processing job"""
    if job_id not in JOBS_STORE:
        raise HTTPException(status_code=404, detail="Job not found")

    # Get the stored job information
    job_info = JOBS_STORE[job_id]

    # Get the Celery task status
    task_id = job_info["task_id"]
    task_result = AsyncResult(task_id, app=celery_app)

    # Update the job status
    status = task_result.status
    JOBS_STORE[job_id]["status"] = status

    # If the task is successful, extract the output filename
    result = None
    if status == "SUCCESS" and task_result.result:
        result = task_result.result
        if isinstance(result, dict) and result.get("output_file"):
            JOBS_STORE[job_id]["output_file"] = result.get("output_file")

    return JobResponse(job_id=job_id, status=status, task_id=task_id, result=result)


@router.get("/jobs/{job_id}/result")
async def get_job_result(job_id: str):
    """Get the result file of a completed processing job"""
    if job_id not in JOBS_STORE:
        raise HTTPException(status_code=404, detail="Job not found")

    job = JOBS_STORE[job_id]

    if job["status"] != "SUCCESS":
        raise HTTPException(
            status_code=400,
            detail=f"Job not completed successfully. Current status: {job['status']}",
        )

    if not job["output_file"]:
        raise HTTPException(status_code=404, detail="Output file not found")

    file_path = os.path.join(OUTPUT_DIR, job["output_file"])
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(
        path=file_path,
        filename=job["output_file"],
        media_type="application/octet-stream",
    )


@router.get("/health")
async def health_check() -> dict:
    """Check if the API and its dependencies are healthy"""
    # Check Redis connection
    redis_healthy = False
    try:
        ping = celery_app.backend.client.ping()
        redis_healthy = ping
    except Exception as e:
        logger.warning(f"Redis health check failed: {e}")

    return {
        "status": "healthy",
        "redis": "healthy" if redis_healthy else "unhealthy",
        "timestamp": time.time(),
    }
