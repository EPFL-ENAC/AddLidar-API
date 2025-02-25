from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError
import logging
from fastapi.encoders import jsonable_encoder
import subprocess
from src.api.models import PointCloudRequest, ProcessPointCloudResponse

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

        # process = subprocess.run(['point_cloud_processor'] + cli_args, 
        #                        capture_output=True, 
        #                        text=True)
        # if process.returncode != 0:
        #     raise HTTPException(status_code=400, 
        #                       detail=f"Processing failed: {process.stderr}")
        
        # return {"status": "success", "output": process.stdout}
        return ProcessPointCloudResponse(
            status="success",
            output="Processed point cloud data"
        )
    except ValidationError as e:
        logger.error(f"Validation error: {str(e)}")
        raise HTTPException(
            status_code=422,
            detail=str(e)
        )
    except ValueError as e:
        logger.error(f"Value error: {str(e)}")
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing point cloud: {str(e)}"
        )

@router.get("/health")
async def health_check() -> dict:
    return {"status": "healthy"}