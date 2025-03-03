from fastapi import APIRouter, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response, FileResponse
from pydantic import ValidationError
import logging
import os
from fastapi.encoders import jsonable_encoder
import time
import asyncio
import uuid
from src.services.k8s_addlidarmanager import (
    process_point_cloud, 
    register_websocket, 
    start_watching_job,
    job_statuses as k8s_job_statuses,
    create_k8s_job,
    active_connections
)
from src.api.models import PointCloudRequest, ProcessPointCloudResponse

# Replace Docker service import with Kubernetes service
# from src.services.k8s_addlidarmanager import process_point_cloud, track_job_status, start_watching_job
from src.config.settings import settings
from src.services.parse_docker_error import parse_cli_error

router = APIRouter()
logger = logging.getLogger("uvicorn")


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
            full_output_path = os.path.join(
                settings.DEFAULT_OUTPUT_ROOT, output_file_path
            )
            logger.info(f"outputfile: {output_file_path}")
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


@router.get("/health")
async def health_check() -> dict:
    """Check if the API and its dependencies are healthy"""
    return {
        "status": "healthy",
        "timestamp": time.time(),
    }


# In-memory storage for job statuses (Use a DB for production)

@router.post("/start-job/")
async def start_job(background_tasks: BackgroundTasks):
    """Starts a Kubernetes job and begins watching its status.
    
    Returns:
        dict: Job name and WebSocket URL for status tracking
    """
    # Generate a unique job name
    job_name = f"job-{uuid.uuid4().hex[:8]}"
    
    # Create the actual Kubernetes job
    result = create_k8s_job(job_name)
    
    # Start watching the job status in a separate thread
    start_watching_job(job_name, namespace=settings.NAMESPACE)
    
    # Return the job name and the WebSocket URL for status tracking
    return {
        "job_name": job_name, 
        "status_url": f"/ws/job-status/{job_name}"
    }

@router.get("/job-status/{job_name}")
async def get_job_status(job_name: str):
    """Fetch the current status of a job.
    
    Args:
        job_name: Name of the job to check
        
    Returns:
        JSONResponse: Current job status
    """
    # Get status from the k8s job status tracking dictionary
    status = k8s_job_statuses.get(job_name, {})
    
    if not status:
        return JSONResponse(
            status_code=404,
            content={"job_name": job_name, "status": "Not Found"}
        )
    
    return JSONResponse(content={
        "job_name": job_name, 
        "status": status.get("status", "Unknown"),
        "message": status.get("message", "")
    })

@router.websocket("/ws/job-status/{job_name}")
async def websocket_endpoint(websocket: WebSocket, job_name: str) -> None:
    """WebSocket endpoint for real-time job status updates.
    
    Args:
        websocket: WebSocket connection
        job_name: Name of the job to track
    """
    try:
        # Accept the connection first
        await websocket.accept()
        
        # Log connection established
        logger.info(f"WebSocket connection established for job {job_name}")
        
        # Send initial status message
        initial_status = k8s_job_statuses.get(job_name, {"status": "Pending", "message": f"Tracking job: {job_name}"})
        await websocket.send_json({
            "job_name": job_name,
            "status": initial_status.get("status", "Pending"),
            "message": initial_status.get("message", f"Tracking job: {job_name}")
        })
        
        # Register this WebSocket with the job watcher
        register_websocket(job_name, websocket)
        
        # Keep the connection alive with a ping-pong mechanism
        while True:
            try:
                # Wait for messages from the client with a timeout
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                
                # If client sends a close message, close the connection gracefully
                if data == "close":
                    logger.info(f"Client requested to close WebSocket for job {job_name}")
                    await websocket.close()
                    break
                    
                # Send current status as a response to any message
                current_status = k8s_job_statuses.get(job_name, {"status": "Unknown"})
                await websocket.send_json({
                    "job_name": job_name,
                    "status": current_status.get("status", "Unknown"),
                    "message": current_status.get("message", "")
                })
                    
            except asyncio.TimeoutError:
                # Send a ping to keep the connection alive
                current_status = k8s_job_statuses.get(job_name, {"status": "Unknown"})
                try:
                    await websocket.send_json({"type": "ping", "job_name": job_name})
                except Exception:
                    logger.warning(f"Failed to send ping to WebSocket for job {job_name}, closing connection")
                    break
                    
    except WebSocketDisconnect:
        logger.info(f"WebSocket client disconnected from job {job_name}")
    except Exception as e:
        logger.error(f"WebSocket error for job {job_name}: {str(e)}", exc_info=True)
    finally:
        # Always try to close the connection and clean up
        try:
            if job_name in active_connections and active_connections[job_name] == websocket:
                del active_connections[job_name]
                logger.info(f"Cleaned up WebSocket connection for job {job_name}")
        except Exception as e:
            logger.error(f"Error cleaning up WebSocket for job {job_name}: {str(e)}")
            
        try:
            await websocket.close()
        except Exception:
            pass