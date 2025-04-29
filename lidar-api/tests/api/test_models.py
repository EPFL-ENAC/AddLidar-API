import pytest
from pathlib import Path
from src.api.models import PointCloudRequest


def mock_file_is_file(self):
    valid_paths = [
        "/data/0001_Mission_Root/TEST_GENERATED/all_grouped_high_veg_10th_point.las",
        "/data/test.las",
    ]
    path_str = str(self)
    return path_str in valid_paths or path_str.replace("\\", "/") in valid_paths


@pytest.fixture(autouse=True)
def setup_mock_path(mocker):
    """Setup mock for Path.is_file() method"""
    mocker.patch.object(Path, "is_file", mock_file_is_file)


def test_file_path_validator():
    """Test the file path validator with various inputs"""
    # Test valid absolute path
    absolute_path = Path(
        "/0001_Mission_Root/TEST_GENERATED/all_grouped_high_veg_10th_point.las"
    )
    valid_path = Path(
        "/data/0001_Mission_Root/TEST_GENERATED/all_grouped_high_veg_10th_point.las"
    )
    request = PointCloudRequest(file_path=absolute_path)
    assert request.file_path == valid_path
