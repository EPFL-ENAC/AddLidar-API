import pytest
from unittest.mock import MagicMock, patch
from kubernetes import client
import os
from main_addlidarmanager import get_settings, process_point_cloud

def test_get_settings_defaults():
    """Test that default settings are returned correctly"""
    settings = get_settings()
    assert settings["IMAGE_NAME"] == "ghcr.io/epfl-enac/lidardatamanager"
    assert settings["IMAGE_TAG"] == "latest"
    assert settings["NAMESPACE"] == "default"
    assert settings["MOUNT_PATH"] == "/data"
    assert settings["PVC_NAME"] == "lidar-data-pvc"
    assert settings["ROOT_VOLUME"] == "pvc:lidar-data-pvc"
    assert settings["JOB_TIMEOUT"] == 300

def test_get_settings_env_override():
    """Test that environment variables override defaults"""
    with patch.dict(os.environ, {
        "IMAGE_NAME": "test-image",
        "PVC_NAME": "test-pvc",
        "JOB_TIMEOUT": "600"
    }):
        settings = get_settings()
        assert settings["IMAGE_NAME"] == "test-image"
        assert settings["PVC_NAME"] == "test-pvc"
        assert settings["ROOT_VOLUME"] == "pvc:test-pvc"
        assert settings["JOB_TIMEOUT"] == 600

@patch('kubernetes.config.load_kube_config')
@patch('kubernetes.client.BatchV1Api')
@patch('kubernetes.client.CoreV1Api')
@patch('kubernetes.watch.Watch')
def test_process_point_cloud_success(mock_watch, mock_core_v1, mock_batch_v1, mock_load_config):
    """Test successful point cloud processing"""
    # Setup mocks
    mock_job = MagicMock()
    mock_job.metadata.name = "test-job"
    mock_job.status.succeeded = True
    
    mock_watch_instance = MagicMock()
    mock_watch_instance.stream.return_value = [{"object": mock_job}]
    mock_watch.return_value = mock_watch_instance
    
    mock_pods = MagicMock()
    mock_pods.items = [MagicMock(metadata=MagicMock(name="test-pod"))]
    mock_core_v1.return_value.list_namespaced_pod.return_value = mock_pods
    mock_core_v1.return_value.read_namespaced_pod_log.return_value = "Success"
    
    # Test
    output, exit_code, output_path = process_point_cloud(["test.las", "--format=pcd-ascii"])
    
    # Verify
    assert exit_code == 0
    assert output == b"Success"
    assert output_path is not None