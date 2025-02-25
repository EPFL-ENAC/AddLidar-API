from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from pydantic import ValidationError
import logging
from fastapi.encoders import jsonable_encoder
from src.api.models import PointCloudRequest, ProcessPointCloudResponse
from src.services.docker_service import process_point_cloud as docker_process_point_cloud
from src.services.parse_docker_error import parse_cli_error, to_json, to_html

router = APIRouter()
logger = logging.getLogger("uvicorn")

from src.config.settings import settings

@router.get("/process-point-cloud")
async def process_point_cloud(
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
    incrs: str | None = None
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
            incrs=incrs
        )
        
        cli_args = request.to_cli_arguments()
        logger.debug(f"CLI arguments: {cli_args}")

        # Process point cloud using docker service
        output, exit_code = docker_process_point_cloud(file_path=settings.DOCKER_VOLUME, cli_args=cli_args)
        
        # If exit code is 0, return the binary data
        if exit_code == 0:
            # Try to determine content type based on format
            content_type = "application/octet-stream"
            if format:
                if format.lower() in ["pcd-ascii", "pcd-binary"]:
                    content_type = "text/plain" if format == "pcd-ascii" else "application/octet-stream"
                elif format.lower() in ["lasv14", "las"]:
                    content_type = "application/octet-stream"
            
            return Response(content=output, media_type=content_type)
        
        # If there was an error, return JSON response
        error_message = output.decode('utf-8', errors='replace')
        return JSONResponse(
            status_code=400,
            content=ProcessPointCloudResponse(
                status="error",
                error_type="cli_error",
                error_details={
                    "message": error_message,
                    "cli_args": cli_args,
                    "exit_code": exit_code
                },
                output=error_message
            ).dict()
        )

    except ValidationError as e:
        logger.error(f"Validation error: {str(e)}")
        return JSONResponse(
            status_code=400,
            content=ProcessPointCloudResponse(
                status="error",
                output=str(e),
                error_type="validation_error",
                error_details={"errors": jsonable_encoder(e.errors())}
            ).dict()
        )
    except ValueError as e:
        logger.error(f"Value error: {str(e)}")
        return JSONResponse(
            status_code=400,
            content=ProcessPointCloudResponse(
                status="error",
                output=str(e),
                error_type="value_error",
                error_details={"message": str(e)}
            ).dict()
        )
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return JSONResponse(
            status_code=500,
            content=ProcessPointCloudResponse(
                status="error",
                output=str(e),
                error_type="internal_error",
                error_details={"message": "An unexpected error occurred"}
            ).dict()
        )

@router.get("/health")
async def health_check() -> dict:
    return {"status": "healthy"}