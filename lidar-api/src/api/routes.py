from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError
import logging
from fastapi.encoders import jsonable_encoder
import subprocess
from src.api.models import PointCloudRequest, ProcessPointCloudResponse
from src.services.docker_service import process_point_cloud as docker_process_point_cloud
from src.services.parse_docker_error import parse_cli_error, to_json, to_html

router = APIRouter()
logger = logging.getLogger("uvicorn")

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
) -> ProcessPointCloudResponse:
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
        logger.debug(f"Received PointCloudRequest: {jsonable_encoder(request)}")
        logger.debug(f"CLI arguments: {cli_args}")

        # Process point cloud using docker service
        output = docker_process_point_cloud(file_path='data', cli_args=cli_args)
        
        # Check for CLI parser errors
        if "PARSE ERROR:" in output or "Brief USAGE:" in output:
            parsed_data = parse_cli_error(output)
            json_output = to_json(parsed_data)
            # Extract the specific error message and clean it up
            error_message = parsed_data.get("error_message", "Unknown parser error")
            if isinstance(error_message, str):
                error_message = error_message.replace("PARSE ERROR: ", "").strip()
            
            # Get the usage example for the specific flag if available
            usage_info = parsed_data.get("usage", "").strip()
            arguments_info = parsed_data.get("arguments", [])
            
            return ProcessPointCloudResponse(
                status="error",
                error_type="cli_parser_error",
                error_details={
                    "message": error_message,
                    "cli_args": cli_args,
                    # "usage": usage_info,
                    "available_arguments": [
                        {
                            "flag": arg["flag"],
                            "description": arg["description"]
                        }
                        for arg in arguments_info
                    ],
                    "help_text": parsed_data.get("help_text", "")
                },
                output="CLI ERROR",#output,
            )

        return ProcessPointCloudResponse(
            status="success",
            output=output
        )
    except ValidationError as e:
        logger.error(f"Validation error: {str(e)}")
        return ProcessPointCloudResponse(
            status="error",
            output=str(e),
            error_type="validation_error",
            error_details={"errors": e.errors()}
        )
    except ValueError as e:
        logger.error(f"Value error: {str(e)}")
        return ProcessPointCloudResponse(
            status="error",
            output=str(e),
            error_type="value_error",
            error_details={"message": str(e)}
        )
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return ProcessPointCloudResponse(
            status="error",
            output=str(e),
            error_type="internal_error",
            error_details={"message": "An unexpected error occurred"}
        )

@router.get("/health")
async def health_check() -> dict:
    return {"status": "healthy"}