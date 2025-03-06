from typing import Optional, List, Tuple
from pydantic import BaseModel, Field, field_validator
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
    returns: Optional[int] = Field(None, description="Max return index", ge=-1)
    number: Optional[int] = Field(
        None, description="Max number of points in output", ge=-1
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
        if v.is_absolute():
            # Prepend /data to absolute paths if not already starting with /data
            if not str(v).startswith("/data"):
                v = Path("/data") / v.relative_to("/")
            else:
                v = v
        else:
            raise ValueError("Path must be absolute")
        # We don't check if file exist, since it may not exist yet because of docker volumes
        return v

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: Optional[str]) -> Optional[str]:
        if not v:
            return v
        valid_formats = [
            "pcd-ascii",
            "lasv14",
            "pcd-bin",
            "lasv13",
            "lasv12",
        ]  # Add more valid formats
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

    def _add_flag_arg(self, args: List[str], condition: bool, flag: str) -> None:
        """Add a simple flag argument if condition is True"""
        if condition:
            args.append(flag)

    def _add_value_arg(self, args: List[str], value: Optional[any], flag: str) -> None:
        """Add a flag with value if value is not None"""
        if value is not None:
            args.append(f"{flag}={value}")

    def to_cli_arguments(self) -> List[str]:
        """Convert the model to CLI arguments"""
        args = [str(self.file_path)]

        # Handle attribute removals
        if self.remove_attribute:
            for attr in self.remove_attribute:
                args.extend(["--remove_attribute", attr])

        # Add simple flags
        self._add_flag_arg(args, self.remove_all_attributes, "--remove_all_attributes")
        self._add_flag_arg(args, self.remove_color, "--remove_color")

        # Add value arguments
        self._add_value_arg(args, self.format, "-f")
        self._add_value_arg(args, self.line, "-l")
        self._add_value_arg(args, self.returns, "-r")
        self._add_value_arg(args, self.number, "-n")
        self._add_value_arg(args, self.density, "-d")
        self._add_value_arg(args, self.outcrs, "--outcrs")
        self._add_value_arg(args, self.incrs, "--incrs")

        # Handle ROI separately
        if self.roi:
            args.append(f'--roi={",".join(map(str, self.roi))}')

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
