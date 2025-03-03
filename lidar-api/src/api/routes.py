from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import JSONResponse, Response, FileResponse
from pydantic import ValidationError
import logging
import os
from fastapi.encoders import jsonable_encoder
import time

from src.api.models import PointCloudRequest, ProcessPointCloudResponse

# Replace Docker service import with Kubernetes service
from src.services.k8s_addlidarmanager import process_point_cloud
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
    # Check Redis connection
    redis_healthy = False
    # try:
    #     ping = celery_app.backend.client.ping()
    #     redis_healthy = ping
    # except Exception as e:
    #     logger.warning(f"Redis health check failed: {e}")

    return {
        "status": "healthy",
        "redis": "healthy" if redis_healthy else "unhealthy",
        "timestamp": time.time(),
    }
