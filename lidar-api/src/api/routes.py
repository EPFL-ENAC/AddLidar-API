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
    register_websocket,
    start_watching_job,
    job_statuses as k8s_job_statuses,
    create_k8s_job,
    active_connections,
    watch_control,
    delete_k8s_job,
)
from src.api.models import PointCloudRequest, ProcessPointCloudResponse

# Replace Docker service import with Kubernetes service
from src.config.settings import settings
from src.services.parse_docker_error import parse_cli_error

router = APIRouter()
logger = logging.getLogger("uvicorn")

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


# Add cleanup task to delete the output file after response is sent
def remove_output_file(file_path: str) -> None:
    try:
        logger.info("Removing output file: %s", file_path)
        if os.path.exists(file_path):
            os.unlink(file_path)
            logger.info(f"Deleted temporary output file: {file_path}")
    except Exception as e:
        logger.error(f"Error deleting output file {file_path}: {str(e)}")


async def return_file_from_output(
    file_format: str, output_file_path: str
) -> FileResponse:
    # Try to determine content type based on format
    content_type = "application/octet-stream"

    # Default extension and content type
    extension = ".bin"
    content_type = "application/octet-stream"

    # If format is specified, get the appropriate extension and content type
    if file_format and file_format.lower() in format_to_extension:
        extension, content_type = format_to_extension[file_format.lower()]

    # Ensure output_file_path is a string
    if not isinstance(output_file_path, str):
        raise TypeError("output_file_path must be a string")

    # Get the full path to the output file
    full_output_path = os.path.join(settings.DEFAULT_OUTPUT_ROOT, output_file_path)
    logger.info(f"outputfile: {output_file_path}")
    logger.info(f"full_output_path: {full_output_path}")
    # Create a filename with the appropriate extension
    original_filename = os.path.basename(output_file_path)
    base_filename = os.path.splitext(original_filename)[0]
    file_name = f"{base_filename}{extension}"

    # Log the file name we're serving
    logger.debug(f"Serving file {file_name} with content type {content_type}")
    return FileResponse(
        path=full_output_path, media_type=content_type, filename=file_name
    )


@router.get("/health")
async def health_check() -> dict:
    """Check if the API and its dependencies are healthy"""
    return {
        "status": "healthy",
        "timestamp": time.time(),
    }


# In-memory storage for job statuses (Use a DB for production)


@router.delete("/stop-job/{job_name}")
async def stop_job(job_name: str):
    """Stop a Kubernetes job by deleting it.
    Also stops the job status watcher for the job.
    Also delete the output file if it exists.
    Also delete the job status from the in-memory storage.
    Also delete the job name from the active connections.

    Args:
        job_name: Name of the job to stop

    Returns:
        dict: Job name and status message
    """
    try:
        # Stop the Kubernetes job
        # Assuming you have a function `delete_k8s_job` to delete the job
        delete_k8s_job(job_name, namespace=settings.NAMESPACE)
        logger.info(f"Stopped Kubernetes job: {job_name}")

        # Delete the output file if it exists
        job_status = k8s_job_statuses.get(job_name, {})
        output_file_path = job_status.get("output_path")
        if output_file_path:
            full_output_path = os.path.join(
                settings.DEFAULT_OUTPUT_ROOT, output_file_path
            )
            if os.path.exists(full_output_path):
                os.unlink(full_output_path)
                logger.info(f"Deleted output file for job: {job_name}")

        # Delete the job status from the in-memory storage
        if job_name in k8s_job_statuses:
            del k8s_job_statuses[job_name]
            logger.info(f"Deleted job status for job: {job_name}")

        # Stop the job status watcher
        if job_name in active_connections:
            websocket = active_connections[job_name]
            try:
                if websocket.client_state == 1:
                    await websocket.close
            except Exception as e:
                logger.error(f"Error closing WebSocket for job {job_name}: {str(e)}")
                pass
            del active_connections[job_name]
            logger.info(f"Stopped job status watcher for job: {job_name}")

        return {"job_name": job_name, "status": "Job stopped successfully"}

    except Exception as e:
        logger.error(f"Error stopping job {job_name}: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "job_name": job_name,
                "status": "Error",
                "message": f"Error stopping job: {str(e)}",
            },
        )


@router.post("/start-job/")
async def start_job(payload: PointCloudRequest):
    """Starts a Kubernetes job and begins watching its status.

    Args:
        payload: PointCloudRequest containing job parameters

    Returns:
        dict: Job name and WebSocket URL for status tracking
    """
    try:
        # Generate a unique job name
        job_name = f"job-{uuid.uuid4().hex[:8]}"

        # Convert payload to CLI arguments
        cli_args = payload.to_cli_arguments()
        logger.debug(f"CLI arguments for job {job_name}: {cli_args}")

        # Create the actual Kubernetes job
        job_name = create_k8s_job(job_name, cli_args)

        # Start watching the job status in a separate thread
        start_watching_job(job_name, namespace=settings.NAMESPACE)

        # Return the job name and the WebSocket URL for status tracking
        return {"job_name": job_name, "status_url": f"/ws/job-status/{job_name}"}
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
            status_code=404, content={"job_name": job_name, "status": "Not Found"}
        )

    return JSONResponse(
        content={
            "job_name": job_name,
            "status": status.get("status", "Unknown"),
            "message": status.get("message", ""),
        }
    )


@router.get("/download/{job_name}")
async def get_job_file(
    background_tasks: BackgroundTasks, job_name: str
) -> FileResponse:
    """Download the output file from a job."""
    try:

        job_status = k8s_job_statuses.get(job_name, {})
        job_status_args = job_status.get("cli_args", [])
        file_format = next(
            (arg.split("=")[1] for arg in job_status_args if arg.startswith("-f=")),
            None,
        )
        if file_format not in format_to_extension:
            file_format = ".bin"
        logger.info(f"job_status: {job_status}")
        output_file_path = job_status.get("output_path")
        logger.info(f"output_file_path: {output_file_path}")
        if not job_status or not output_file_path:
            return JSONResponse(
                status_code=404,
                content={
                    "job_name": job_name,
                    "status": "Not Found",
                    "message": f"Job not found or output file ({output_file_path}) not available",
                },
            )
        return await return_file_from_output(
            file_format=file_format,
            output_file_path=output_file_path,
        )
    except Exception as e:
        logger.error(f"Error downloading file for job {job_name}: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "job_name": job_name,
                "status": "Error",
                "message": f"Error downloading file: {str(e)}",
            },
        )
    # finally:
    #     # Remove the output file after sending the response
    #     if output_file_path:
    #         full_output_path = os.path.join(
    #             settings.DEFAULT_OUTPUT_ROOT, output_file_path
    #         )
    #         background_tasks.add_task(remove_output_file, full_output_path)


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
        initial_status = k8s_job_statuses.get(
            job_name, {"status": "Pending", "message": f"Tracking job: {job_name}"}
        )
        await websocket.send_json(
            {
                "job_name": job_name,
                "status": initial_status.get("status", "Pending"),
                "message": initial_status.get("message", f"Tracking job: {job_name}"),
            }
        )

        # Register this WebSocket with the job watcher
        register_websocket(job_name, websocket)

        # Keep the connection alive with a ping-pong mechanism
        while True:
            try:
                # Wait for messages from the client with a timeout
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)

                # If client sends a close message, close the connection gracefully
                if data == "close":
                    logger.info(
                        f"Client requested to close WebSocket for job {job_name}"
                    )
                    await websocket.close()
                    break

                # Send current status as a response to any message
                current_status = k8s_job_statuses.get(job_name, {"status": "Unknown"})
                await websocket.send_json(
                    {
                        "job_name": job_name,
                        "status": current_status.get("status", "Unknown"),
                        "message": current_status.get("message", ""),
                    }
                )

            except asyncio.TimeoutError:
                # Send a ping to keep the connection alive
                current_status = k8s_job_statuses.get(job_name, {"status": "Unknown"})
                try:
                    await websocket.send_json({"type": "ping", "job_name": job_name})
                except Exception:
                    logger.warning(
                        f"Failed to send ping to WebSocket for job {job_name}, closing connection"
                    )
                    break

    except WebSocketDisconnect:
        logger.info(f"WebSocket client disconnected from job {job_name}")
    except Exception as e:
        logger.error(f"WebSocket error for job {job_name}: {str(e)}", exc_info=True)
    finally:
        # Always try to close the connection and clean up
        try:
            if (
                job_name in active_connections
                and active_connections[job_name] == websocket
            ):
                del active_connections[job_name]
                logger.info(f"Cleaned up WebSocket connection for job {job_name}")
        except Exception as e:
            logger.error(f"Error cleaning up WebSocket for job {job_name}: {str(e)}")

        try:
            await websocket.close()
        except Exception:
            pass


@router.get("/ws/health")
async def websocket_health_check() -> dict:
    """Check if the WebSocket connections are healthy"""
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "active_connections": (
            len(active_connections.keys())
            if hasattr(active_connections, "keys")
            else (
                sum(1 for _ in active_connections)
                if hasattr(active_connections, "__iter__")
                else 0
            )
        ),
        "job_statuses": len(k8s_job_statuses),
        "namespace": settings.NAMESPACE,
        "watch_connections": (
            len(watch_control.keys())
            if hasattr(watch_control, "keys")
            else (
                sum(1 for _ in watch_control)
                if hasattr(watch_control, "__iter__")
                else 0
            )
        ),
    }
