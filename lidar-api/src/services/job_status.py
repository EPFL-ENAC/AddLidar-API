import logging
from typing import Dict, Any, List, Optional
from kubernetes import client
from pydantic import BaseModel
from datetime import datetime

logger = logging.getLogger(__name__)

# Dictionary to store job statuses in memory
job_statuses: Dict[str, Dict[str, Any]] = {}


class JobStatusInfo(BaseModel):
    """Model representing detailed job status information."""

    name: str
    active: Optional[int] = None
    succeeded: Optional[int] = None
    failed: Optional[int] = None
    start_time: Optional[datetime] = None
    completion_time: Optional[datetime] = None
    conditions: Optional[List[Dict[str, Any]]] = None
    tracked_status: Optional[Dict[str, Any]] = None
    pods_status: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None
    code: Optional[int] = None


class KubernetesJobStatusManager:
    """Service to manage and retrieve Kubernetes job statuses."""

    def __init__(self):
        """Initialize the job status manager."""
        self.batch_v1 = client.BatchV1Api()
        self.core_v1 = client.CoreV1Api()

    def get_job_status(
        self, job_name: str, namespace: str = "default"
    ) -> Dict[str, Any]:
        """
        Get the current status of a Kubernetes job.

        Args:
            job_name: Name of the job to retrieve status for
            namespace: Kubernetes namespace where the job exists

        Returns:
            Dict containing job status information
        """
        try:
            job = self.batch_v1.read_namespaced_job(name=job_name, namespace=namespace)

            # Extract relevant status information
            status_info = {
                "name": job_name,
                "active": job.status.active,
                "succeeded": job.status.succeeded,
                "failed": job.status.failed,
                "start_time": job.status.start_time,
                "completion_time": job.status.completion_time,
                "conditions": None,
            }

            # Add conditions if present
            if job.status.conditions:
                status_info["conditions"] = [
                    {
                        "type": condition.type,
                        "status": condition.status,
                        "reason": condition.reason,
                        "message": condition.message,
                        "last_transition_time": condition.last_transition_time,
                    }
                    for condition in job.status.conditions
                ]

            # Add stored job status from our internal tracking
            if job_name in job_statuses:
                status_info["tracked_status"] = job_statuses[job_name]

            return status_info

        except client.exceptions.ApiException as e:
            if e.status == 404:
                logger.warning(f"Job {job_name} not found in namespace {namespace}")
                return {"name": job_name, "error": "Job not found", "code": 404}
            else:
                logger.error(f"Error getting job status: {str(e)}")
                return {"name": job_name, "error": str(e), "code": e.status}

        except Exception as e:
            logger.error(
                f"Unexpected error getting job status for {job_name}: {str(e)}"
            )
            return {"name": job_name, "error": str(e)}

    def get_detailed_job_status(
        self, job_name: str, namespace: str = "default"
    ) -> Dict[str, Any]:
        """
        Get detailed status of a job by examining its pods

        Args:
            job_name: Name of the job to retrieve detailed status for
            namespace: Kubernetes namespace where the job exists

        Returns:
            Dict with comprehensive status information including pod details
        """
        # Get basic job status
        job_status = self.get_job_status(job_name, namespace)

        try:
            # Get pods associated with this job
            pod_list = self.core_v1.list_namespaced_pod(
                namespace=namespace, label_selector=f"job-name={job_name}"
            )

            # Add detailed pod status information
            pods_status = []
            for pod in pod_list.items:
                pod_status = {
                    "name": pod.metadata.name,
                    "phase": pod.status.phase,  # Pending, Running, Succeeded, Failed, Unknown
                    "conditions": [
                        {"type": cond.type, "status": cond.status}
                        for cond in pod.status.conditions or []
                    ],
                    "container_statuses": [],
                }

                # Add container status info
                if pod.status.container_statuses:
                    for container in pod.status.container_statuses:
                        state = {"name": container.name}

                        if container.state.running:
                            state["state"] = "Running"
                            state["started_at"] = container.state.running.started_at
                        elif container.state.terminated:
                            state["state"] = "Terminated"
                            state["exit_code"] = container.state.terminated.exit_code
                            state["reason"] = container.state.terminated.reason
                            state["message"] = container.state.terminated.message
                        elif container.state.waiting:
                            state["state"] = "Waiting"
                            state["reason"] = container.state.waiting.reason

                        pod_status["container_statuses"].append(state)

                pods_status.append(pod_status)

            # Add detailed pod info to job status
            job_status["pods_status"] = pods_status

            return job_status

        except Exception as e:
            logger.error(
                f"Error getting detailed pod status for job {job_name}: {str(e)}"
            )
            job_status["pods_error"] = str(e)
            return job_status

    def interpret_job_status(self, job_status: Dict[str, Any]) -> str:
        """
        Interpret the job status into a simple string status.

        Args:
            job_status: The job status dictionary from get_job_status or get_detailed_job_status

        Returns:
            String representing the current state: 'NotFound', 'Error', 'Pending', 'Creating',
            'Running', 'Completed', or 'Failed'
        """
        # Check sequential status conditions in order of priority

        # 1. Check error states
        if job_status.get("error"):
            return "NotFound" if job_status.get("code") == 404 else "Error"

        # 2. Check terminal states
        if job_status.get("succeeded"):
            return "Completed"

        if job_status.get("failed"):
            return "Failed"

        # 3. Check status from conditions
        if job_status.get("conditions"):
            for condition in job_status.get("conditions", []):
                if (
                    condition.get("type") == "Complete"
                    and condition.get("status") == "True"
                ):
                    return "Completed"
                if (
                    condition.get("type") == "Failed"
                    and condition.get("status") == "True"
                ):
                    return "Failed"

        # 4. Check active job states
        if job_status.get("active"):
            return self._get_active_job_status(job_status)

        # 5. Default state
        return "Pending"

    def _get_active_job_status(self, job_status: Dict[str, Any]) -> str:
        """
        Determine the status of an active job based on its pod status.

        Args:
            job_status: The job status dictionary

        Returns:
            String representing the active job state: 'Creating', 'Running'
        """
        # If we don't have pod details, default to Running
        if not job_status.get("pods_status"):
            return "Running"

        # Check each pod's status
        for pod in job_status["pods_status"]:
            if pod.get("phase") == "Running":
                # Check if all containers are ready
                container_statuses = pod.get("container_statuses", [])
                if not container_statuses or all(
                    container.get("state") == "Running"
                    for container in container_statuses
                ):
                    return "Running"
                return "Creating"

            if pod.get("phase") == "Pending":
                return "Creating"

        # Default to Running if active but couldn't determine status from pods
        return "Running"


# Create singleton instance
job_status_manager = KubernetesJobStatusManager()
