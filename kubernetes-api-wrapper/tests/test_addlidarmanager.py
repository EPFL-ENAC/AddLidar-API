import unittest
from unittest.mock import patch, MagicMock, ANY
import os
import sys
import logging
from kubernetes.client.rest import ApiException

# Add the parent directory to sys.path so we can import main_addlidarmanager
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main_addlidarmanager import process_point_cloud, get_settings


class TestAddLidarManager(unittest.TestCase):
    """Test cases for the main_addlidarmanager module."""

    def setUp(self):
        """Set up test environment."""
        # Configure logging to suppress logs during tests
        logging.basicConfig(level=logging.CRITICAL)
        
        # Mock environment variables
        os.environ["IMAGE_NAME"] = "test-image"
        os.environ["IMAGE_TAG"] = "test"
        os.environ["NAMESPACE"] = "test-namespace"
        os.environ["STORAGE_TYPE"] = "emptyDir"
        os.environ["JOB_TIMEOUT"] = "10"
        
    def tearDown(self):
        """Clean up after tests."""
        # Remove test environment variables
        for key in ["IMAGE_NAME", "IMAGE_TAG", "NAMESPACE", "STORAGE_TYPE", "JOB_TIMEOUT", 
                   "PVC_NAME", "HOST_PATH"]:
            if key in os.environ:
                del os.environ[key]
    
    def test_get_settings(self):
        """Test that get_settings correctly loads environment variables."""
        settings = get_settings()
        
        self.assertEqual(settings["IMAGE_NAME"], "test-image")
        self.assertEqual(settings["IMAGE_TAG"], "test")
        self.assertEqual(settings["NAMESPACE"], "test-namespace")
        self.assertEqual(settings["STORAGE_TYPE"], "emptyDir")
        self.assertEqual(settings["JOB_TIMEOUT"], 10)  # Should be converted to int
        
    @patch('main_addlidarmanager.config')
    @patch('main_addlidarmanager.client')
    @patch('main_addlidarmanager.watch')
    def test_process_point_cloud_success(self, mock_watch, mock_client, mock_config):
        """Test successful point cloud processing."""
        # Mock Kubernetes API responses
        mock_batch_v1 = MagicMock()
        mock_core_v1 = MagicMock()
        mock_client.BatchV1Api.return_value = mock_batch_v1
        mock_client.CoreV1Api.return_value = mock_core_v1
        
        # Mock Watch events
        mock_event = {
            "object": MagicMock(
                metadata=MagicMock(name="test-job"),
                status=MagicMock(succeeded=True, failed=False)
            )
        }
        mock_watch.Watch.return_value.stream.return_value = [mock_event]
        
        # Mock pod listing
        mock_pod = MagicMock(metadata=MagicMock(name="test-pod"))
        mock_core_v1.list_namespaced_pod.return_value = MagicMock(items=[mock_pod])
        
        # Mock pod logs
        mock_core_v1.read_namespaced_pod_log.return_value = "Job completed successfully"
        
        # Call the function being tested
        cli_args = ["-i=/data/test.las", "--format=pcd-ascii"]
        output, exit_code, output_path = process_point_cloud(cli_args)
        
        # Assertions
        self.assertEqual(exit_code, 0)
        self.assertIsNotNone(output_path)
        self.assertTrue(output_path.startswith("output/"))
        self.assertEqual(output, b"Job completed successfully")
        
        # Verify API calls
        mock_batch_v1.create_namespaced_job.assert_called_once()
        mock_batch_v1.delete_namespaced_job.assert_called_once()
        mock_core_v1.list_namespaced_pod.assert_called_once()
        mock_core_v1.read_namespaced_pod_log.assert_called_once()
    
    @patch('main_addlidarmanager.config')
    @patch('main_addlidarmanager.client')
    @patch('main_addlidarmanager.watch')
    def test_process_point_cloud_failure(self, mock_watch, mock_client, mock_config):
        """Test failed point cloud processing."""
        # Mock Kubernetes API responses
        mock_batch_v1 = MagicMock()
        mock_core_v1 = MagicMock()
        mock_client.BatchV1Api.return_value = mock_batch_v1
        mock_client.CoreV1Api.return_value = mock_core_v1
        
        # Mock Watch events - job failed
        mock_event = {
            "object": MagicMock(
                metadata=MagicMock(name="test-job"),
                status=MagicMock(succeeded=False, failed=True)
            )
        }
        mock_watch.Watch.return_value.stream.return_value = [mock_event]
        
        # Mock pod listing
        mock_pod = MagicMock(metadata=MagicMock(name="test-pod"))
        mock_core_v1.list_namespaced_pod.return_value = MagicMock(items=[mock_pod])
        
        # Mock pod logs
        mock_core_v1.read_namespaced_pod_log.return_value = "Error: file not found"
        
        # Call the function being tested
        cli_args = ["-i=/data/nonexistent.las", "--format=pcd-ascii"]
        output, exit_code, output_path = process_point_cloud(cli_args)
        
        # Assertions
        self.assertEqual(exit_code, 1)
        self.assertIsNone(output_path)
        self.assertEqual(output, b"Error: file not found")
        
        # Verify API calls
        mock_batch_v1.create_namespaced_job.assert_called_once()
        mock_batch_v1.delete_namespaced_job.assert_called_once()
    
    @patch('main_addlidarmanager.config')
    @patch('main_addlidarmanager.client')
    def test_api_exception_handling(self, mock_client, mock_config):
        """Test handling of Kubernetes API exceptions."""
        # Mock API exception
        mock_client.BatchV1Api.return_value.create_namespaced_job.side_effect = ApiException(
            status=403, reason="Forbidden"
        )
        
        # Call the function being tested
        cli_args = ["-i=/data/test.las", "--format=pcd-ascii"]
        output, exit_code, output_path = process_point_cloud(cli_args)
        
        # Assertions
        self.assertEqual(exit_code, 1)
        self.assertIsNone(output_path)
        self.assertIn(b"Kubernetes job error", output)
        self.assertIn(b"403", output)
    
    @patch('main_addlidarmanager.config')
    @patch('main_addlidarmanager.client')
    @patch('main_addlidarmanager.watch')
    def test_pvc_storage_type(self, mock_watch, mock_client, mock_config):
        """Test PVC storage type configuration."""
        os.environ["STORAGE_TYPE"] = "pvc"
        os.environ["PVC_NAME"] = "test-pvc"
        
        # Mock Kubernetes clients
        mock_batch_v1 = MagicMock()
        mock_core_v1 = MagicMock()
        mock_client.BatchV1Api.return_value = mock_batch_v1
        mock_client.CoreV1Api.return_value = mock_core_v1
        
        # Mock Watch events
        mock_event = {
            "object": MagicMock(
                metadata=MagicMock(name=ANY),  # Using ANY since job name is dynamic
                status=MagicMock(succeeded=True, failed=False)
            )
        }
        mock_watch.Watch.return_value.stream.return_value = [mock_event]
        
        # Mock pod listing and logs
        mock_pod = MagicMock(metadata=MagicMock(name="test-pod"))
        mock_core_v1.list_namespaced_pod.return_value = MagicMock(items=[mock_pod])
        mock_core_v1.read_namespaced_pod_log.return_value = "Success"
        
        # Call the function
        cli_args = ["-i=/data/test.las"]
        process_point_cloud(cli_args)
        
        # Extract the job creation call arguments
        job_spec = mock_batch_v1.create_namespaced_job.call_args[1]['body']
        
        # Assert PVC is used in volumes
        volumes = job_spec.spec.template.spec.volumes
        self.assertTrue(any(vol.persistent_volume_claim is not None for vol in volumes))
        self.assertTrue(any(vol.persistent_volume_claim.claim_name == "test-pvc" for vol in volumes))
    
    @patch('main_addlidarmanager.config')
    @patch('main_addlidarmanager.client')
    @patch('main_addlidarmanager.watch')
    def test_empty_dir_fallback(self, mock_watch, mock_client, mock_config):
        """Test fallback to emptyDir when PVC name is not provided."""
        os.environ["STORAGE_TYPE"] = "pvc"
        os.environ["PVC_NAME"] = ""  # Empty PVC name should trigger fallback
        
        # Similar mocking as before
        mock_batch_v1 = MagicMock()
        mock_core_v1 = MagicMock()
        mock_client.BatchV1Api.return_value = mock_batch_v1
        mock_client.CoreV1Api.return_value = mock_core_v1
        
        mock_event = {
            "object": MagicMock(
                metadata=MagicMock(name=ANY),
                status=MagicMock(succeeded=True, failed=False)
            )
        }
        mock_watch.Watch.return_value.stream.return_value = [mock_event]
        
        mock_pod = MagicMock(metadata=MagicMock(name="test-pod"))
        mock_core_v1.list_namespaced_pod.return_value = MagicMock(items=[mock_pod])
        mock_core_v1.read_namespaced_pod_log.return_value = "Success"
        
        # Call the function
        cli_args = ["-i=/data/test.las"]
        process_point_cloud(cli_args)
        
        # Extract the job creation call arguments
        job_spec = mock_batch_v1.create_namespaced_job.call_args[1]['body']
        
        # Assert emptyDir is used in volumes (fallback behavior)
        volumes = job_spec.spec.template.spec.volumes
        self.assertTrue(any(vol.empty_dir is not None for vol in volumes))


if __name__ == "__main__":
    unittest.main()