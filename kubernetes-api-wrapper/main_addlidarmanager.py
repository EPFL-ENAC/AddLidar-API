from kubernetes import client, config, watch
import time
import base64

def create_and_monitor_job(job_name, namespace, container_image, command):
    """
    Create a Kubernetes job, monitor its status, and retrieve its logs.
    
    Args:
        job_name (str): Name of the job
        namespace (str): Kubernetes namespace
        container_image (str): Docker image to use
        command (list): Command to run in the container
    
    Returns:
        tuple: (job_success, logs)
    """
    # Load Kubernetes config
    # Instead of trying incluster first
    try:
        # Try kubeconfig first (for Docker Desktop)
        config.load_kube_config()
    except:
        # Fall back to in-cluster config
        config.load_incluster_config()
    
    # Create API clients
    batch_v1 = client.BatchV1Api()
    core_v1 = client.CoreV1Api()
    
    # Define job spec
    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(
            name=job_name,
            namespace=namespace
        ),
        spec=client.V1JobSpec(
            template=client.V1PodTemplateSpec(
                spec=client.V1PodSpec(
                    containers=[
                        client.V1Container(
                            name="job-container",
                            image=container_image,
                            command=command
                        )
                    ],
                    restart_policy="Never"
                )
            ),
            backoff_limit=0  # No retries
        )
    )
    
    # Create the job
    print(f"Creating job {job_name} in namespace {namespace}")
    batch_v1.create_namespaced_job(namespace=namespace, body=job)
    
    # Wait for job completion
    job_status = {"succeeded": False, "failed": False}
    w = watch.Watch()
    
    print("Waiting for job to complete...")
    for event in w.stream(batch_v1.list_namespaced_job, namespace=namespace, timeout_seconds=60):
        if event["object"].metadata.name == job_name:
            job_obj = event["object"]
            
            # Check if job has completed
            if job_obj.status.succeeded:
                job_status["succeeded"] = True
                w.stop()
                break
            elif job_obj.status.failed:
                job_status["failed"] = True
                w.stop()
                break
    
    # Get the pod associated with the job
    label_selector = f"job-name={job_name}"
    pods = core_v1.list_namespaced_pod(namespace=namespace, label_selector=label_selector)
    
    if not pods.items:
        return job_status["succeeded"], "No pods found for this job"
    
    pod_name = pods.items[0].metadata.name
    
    # Get the logs
    logs = core_v1.read_namespaced_pod_log(name=pod_name, namespace=namespace)
    
    # Clean up (optional)
    # batch_v1.delete_namespaced_job(name=job_name, namespace=namespace, body=client.V1DeleteOptions())
    
    return job_status["succeeded"], logs


# Example usage (to be called from another deployment)
def example_usage():
    job_name = "sample-job-" + str(int(time.time()))
    namespace = "default"
    container_image = "ubuntu:20.04"
    command = ["/bin/sh", "-c", "echo 'Job starting'; date; sleep 5; echo 'Job completed successfully'; exit 0"]
    
    success, logs = create_and_monitor_job(job_name, namespace, container_image, command)
    
    print(f"Job {'succeeded' if success else 'failed'}")
    print("Logs:")
    print(logs)


# When running in another deployment, call example_usage()
if __name__ == "__main__":
    print("Hello from kubernetes-api-wrapper!")
    example_usage()
