from typing import Optional, List
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Tuple
from pathlib import Path


class PointCloudRequest(BaseModel):
    file_path: Path = Field(
        ..., description="Path to the input point cloud file (inside mounted volume)"
    )
    remove_attribute: Optional[List[str]] = Field(
        None, description="Remove specified attribute(s)"
    )
    remove_all_attributes: bool = Field(
        False, description="Remove all non-geometry attributes"
    )
    remove_color: bool = Field(False, description="Remove color data")
    format: Optional[str] = Field(
        None, description="Output format (pcd-ascii, lasv14, etc.)"
    )
    line: Optional[int] = Field(None, description="Export a specific line index", ge=0)
    returns: Optional[int] = Field(None, description="Max return index", ge=0)
    number: Optional[int] = Field(
        None, description="Max number of points in output", gt=0
    )
    density: Optional[float] = Field(
        None, description="Max density (points per m²)", gt=0.0
    )
    roi: Optional[
        Tuple[float, float, float, float, float, float, float, float, float]
    ] = Field(None, description="Region of interest (x0,y0,z0,dx,dy,dz,rx,ry,rz)")
    outcrs: Optional[str] = Field(None, description="Output CRS (e.g., EPSG:4326)")
    incrs: Optional[str] = Field(None, description="Override input CRS")

    @field_validator("file_path")
    @classmethod
    def validate_file_exists(cls, v: Path) -> Path:
        # Convert to absolute path if relative
        if not v.is_absolute():
            v = Path("/data") / v
        else:
            # Prepend ./data to absolute paths
            v = Path("/data") / v.relative_to("/")

        # # Ensure the path is within the /data directory for security
        try:
            v.relative_to("/data")
        except ValueError:
            raise ValueError("File path must be within the mounted /data volume")

        # We don't check if file exist, since it may not exist yet because of docker volumes
        return v

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: Optional[str]) -> Optional[str]:
        if not v:
            return v
        valid_formats = ["pcd-ascii", "lasv14"]  # Add more valid formats
        if v not in valid_formats:
            raise ValueError(f"Invalid format. Must be one of: {valid_formats}")
        return v

    @field_validator("roi")
    @classmethod
    def validate_roi(
        cls, v: Optional[Tuple[float, ...]]
    ) -> Optional[Tuple[float, ...]]:
        if v and len(v) != 9:
            raise ValueError(
                "ROI must have exactly 9 values (x0,y0,z0,dx,dy,dz,rx,ry,rz)"
            )
        return v

    @field_validator("outcrs", "incrs")
    @classmethod
    def validate_crs(cls, v: Optional[str]) -> Optional[str]:
        if v and not v.startswith("EPSG:"):
            raise ValueError("CRS must be in EPSG format (e.g., EPSG:4326)")
        return v

    def to_cli_arguments(self) -> List[str]:
        """Convert the model to CLI arguments"""
        args = [str(self.file_path)]

        if self.remove_attribute:
            for attr in self.remove_attribute:
                args.extend(["--remove_attribute", attr])
        if self.remove_all_attributes:
            args.append("--remove_all_attributes")
        if self.remove_color:
            args.append("--remove_color")
        if self.format:
            args.append(f"-f={self.format}")
        if self.line is not None:
            args.append(f"-l={self.line}")
        if self.returns is not None:
            args.append(f"-r={self.returns}")
        if self.number is not None:
            args.append(f"-n={self.number}")
        if self.density is not None:
            args.append(f"-d={self.density}")
        if self.roi:
            args.append(f'--roi={",".join(map(str, self.roi))}')
        if self.outcrs:
            args.append(f"--outcrs={self.outcrs}")
        if self.incrs:
            args.append(f"--incrs={self.incrs}")

        return args


class ProcessPointCloudResponse(BaseModel):
    status: str = Field(
        ..., description="Status of the processing operation (success or error)"
    )
    output: str = Field(..., description="Output of the processing operation")
    error_type: Optional[str] = Field(
        None, description="Type of error if status is error"
    )
    error_details: Optional[dict] = Field(
        None, description="Detailed error information"
    )

    model_config = {
        "json_schema_extra": {
            "examples": [{"status": "success", "output": "Processed point cloud data"}]
        }
    }
