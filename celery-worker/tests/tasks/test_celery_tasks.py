import pytest
from unittest.mock import Mock, patch
import docker
import os
import sys
import os.path

# Add the parent directory to sys.path to enable importing from the main module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import the helper function for testing
from test_helpers import lidar_processing_helper


@pytest.fixture
def mock_docker_client():
    """Mock the Docker client for testing"""
    mock_client = Mock()
    mock_container = Mock()
    mock_container.wait.return_value = {"StatusCode": 0}
    mock_container.logs.return_value = b"Processing completed successfully"
    mock_client.containers.run.return_value = mock_container
    return mock_client


class TestLidarProcessing:
    
    @patch("docker.from_env")
    def test_process_lidar_data_success(self, mock_docker, mock_docker_client):
        """Test successful processing of LiDAR data"""
        # Setup
        mock_docker.return_value = mock_docker_client
        job_id = "test-job-123"
        input_file = "/data/test.las"
        parameters = ["-f=lasv14", "--outcrs=EPSG:4326"]
        
        # Execute - call the helper function directly
        result = lidar_processing_helper(job_id, input_file, parameters)
        
        # Assert
        assert result["status"] == "success"
        assert result["job_id"] == job_id
        assert "output_file" in result
        mock_docker_client.containers.run.assert_called_once()
    
    @patch("docker.from_env")
    def test_process_lidar_data_failure(self, mock_docker, mock_docker_client):
        """Test failed processing of LiDAR data"""
        # Setup
        mock_container = Mock()
        mock_container.wait.return_value = {"StatusCode": 1}
        mock_container.logs.return_value = b"Error: Invalid file format"
        mock_docker_client.containers.run.return_value = mock_container
        mock_docker.return_value = mock_docker_client
        
        job_id = "test-job-456"
        input_file = "/data/invalid.las"
        
        # Execute
        result = lidar_processing_helper(job_id, input_file)
        
        # Assert
        assert result["status"] == "error"
        assert result["exit_code"] == 1
        assert "logs" in result
    
    @patch("docker.from_env")
    def test_process_lidar_data_image_not_found(self, mock_docker):
        """Test handling of missing Docker image"""
        # Setup
        mock_docker.side_effect = docker.errors.ImageNotFound("Image not found")
        
        job_id = "test-job-789"
        input_file = "/data/test.las"
        
        # Execute
        result = lidar_processing_helper(job_id, input_file)
        
        # Assert
        assert result["status"] == "error"
        assert "error" in result
        assert "Docker image not found" in result["error"]