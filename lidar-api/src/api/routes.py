from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional

router = APIRouter()


class ProcessPointCloudRequest(BaseModel):
    file_path: str
    remove_attribute: Optional[list[str]] = None
    remove_all_attributes: Optional[bool] = None
    remove_color: Optional[bool] = None
    format: Optional[str] = None
    line: Optional[int] = None
    returns: Optional[int] = None
    number: Optional[int] = None
    density: Optional[float] = None
    roi: Optional[str] = None
    outcrs: Optional[str] = None
    incrs: Optional[str] = None


@router.get("/process-point-cloud")
async def process_point_cloud(request: ProcessPointCloudRequest):
    # Logic to process the point cloud using the Docker service
    # This is a placeholder for the actual implementation
    return {"message": "Processing point cloud", "file_path": request.file_path}

@router.get("/health")
async def health_check():
    return {"status": "healthy"}