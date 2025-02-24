from pydantic import BaseModel
from typing import Optional, List


class ProcessPointCloudRequest(BaseModel):
    file_path: str
    remove_attribute: Optional[List[str]] = None
    remove_all_attributes: Optional[bool] = False
    remove_color: Optional[bool] = False
    format: Optional[str] = None
    line: Optional[int] = None
    returns: Optional[int] = None
    number: Optional[int] = None
    density: Optional[float] = None
    roi: Optional[str] = None
    outcrs: Optional[str] = None
    incrs: Optional[str] = None


class ProcessPointCloudResponse(BaseModel):
    success: bool
    message: str
    data: Optional[dict] = None
