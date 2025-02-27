#!/usr/bin/env python3
"""
Kubernetes PVC utility functions to help with common operations on PVCs.
"""
import logging
import subprocess
from typing import List, Dict, Any, Optional
import json
import os
from pathlib import Path
import time

logger = logging.getLogger(__name__)

class KubernetesPVCError(Exception):
    """Exception raised for errors in Kubernetes PVC operations."""
    pass

def list_pvc_contents(
    pvc_name: str,
    namespace: str = "default",
    path: str = "/",
    timeout: int = 30
) -> List[Dict[str, Any]]:
    """
    List the contents of a Persistent Volume Claim (PVC).
    
    Args:
        pvc_name: Name of the PVC to inspect
        namespace: Kubernetes namespace where the PVC exists
        path: Path inside the PVC to list
        timeout: Maximum time to wait for pod creation in seconds
        
    Returns:
        List of dictionaries containing file information
        
    Raises:
        KubernetesPVCError: If the operation fails
    """
    try:
        pod_name = f"pvc-inspector-{int(time.time())}"
        
        # Create pod spec with the PVC mounted
        pod_spec = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name
            },
            "spec": {
                "containers": [{
                    "name": "inspector",
                    "image": "busybox",
                    "command": ["ls", "-la", "--json", path],
                    "volumeMounts": [{
                        "name": "pvc-volume",
                        "mountPath": "/mnt"
                    }]
                }],
                "volumes": [{
                    "name": "pvc-volume",
                    "persistentVolumeClaim": {
                        "claimName": pvc_name
                    }
                }],
                "restartPolicy": "Never"
            }
        }
        
        # Create pod spec file
        temp_file = Path(f"/tmp/{pod_name}.json")
        with open(temp_file, "w") as f:
            json.dump(pod_spec, f)
            
        # Create the pod
        create_cmd = ["kubectl", "create", "-f", str(temp_file), "-n", namespace]
        subprocess.run(create_cmd, check=True, capture_output=True)
        
        # Wait for pod to be ready
        start_time = time.time()
        while True:
            status_cmd = ["kubectl", "get", "pod", pod_name, "-n", namespace, "-o", "jsonpath={.status.phase}"]
            result = subprocess.run(status_cmd, check=True, capture_output=True, text=True)
            status = result.stdout.strip()
            
            if status == "Running" or status == "Succeeded":
                break
            elif status == "Failed":
                raise KubernetesPVCError(f"Pod {pod_name} failed to start")
            
            if time.time() - start_time > timeout:
                raise KubernetesPVCError(f"Timeout waiting for pod {pod_name} to be ready")
                
            time.sleep(1)
            
        # Get the pod logs which contain the directory listing
        logs_cmd = ["kubectl", "logs", pod_name, "-n", namespace]
        result = subprocess.run(logs_cmd, check=True, capture_output=True, text=True)
        
        # Process the output to return structured data
        # For busybox ls, the output will be text, not JSON, so parse manually
        items = []
        for line in result.stdout.strip().split("\n")[1:]:  # Skip the "total X" line
            if not line.strip():
                continue
                
            parts = line.split()
            if len(parts) < 9:
                continue
                
            # Parse ls -la output format: permissions links owner group size month day time name
            items.append({
                "name": " ".join(parts[8:]),
                "permissions": parts[0],
                "owner": parts[2],
                "group": parts[3],
                "size": int(parts[4]),
                "modified": f"{parts[5]} {parts[6]} {parts[7]}"
            })
            
        return items
        
    except subprocess.CalledProcessError as e:
        raise KubernetesPVCError(f"Command failed: {e.cmd}. Error: {e.stderr.decode('utf-8')}")
    except Exception as e:
        raise KubernetesPVCError(f"Failed to list PVC contents: {str(e)}")
    finally:
        # Clean up
        try:
            subprocess.run(
                ["kubectl", "delete", "pod", pod_name, "-n", namespace, "--grace-period=0", "--force"],
                check=False,
                capture_output=True
            )
        except Exception as e:
            logger.warning(f"Failed to clean up pod {pod_name}: {str(e)}")
            
        if temp_file.exists():
            temp_file.unlink()

def create_file_in_pvc(
    pvc_name: str,
    file_path: str,
    content: str,
    namespace: str = "default",
    timeout: int = 30
) -> None:
    """
    Create a file with specified content in a PVC.
    
    Args:
        pvc_name: Name of the PVC
        file_path: Path where to create the file (relative to PVC root)
        content: Content to write to the file
        namespace: Kubernetes namespace where the PVC exists
        timeout: Maximum time to wait for pod creation in seconds
        
    Raises:
        KubernetesPVCError: If the operation fails
    """
    try:
        pod_name = f"pvc-writer-{int(time.time())}"
        
        # Create temp file with content
        temp_content_file = Path(f"/tmp/{pod_name}-content")
        with open(temp_content_file, "w") as f:
            f.write(content)
            
        # Create pod spec
        pod_spec = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name
            },
            "spec": {
                "containers": [{
                    "name": "writer",
                    "image": "busybox",
                    "command": ["sh", "-c", f"cat /tmp/content > /mnt/{file_path}"],
                    "volumeMounts": [
                        {
                            "name": "pvc-volume",
                            "mountPath": "/mnt"
                        },
                        {
                            "name": "content-volume",
                            "mountPath": "/tmp/content",
                            "subPath": pod_name + "-content"
                        }
                    ]
                }],
                "volumes": [
                    {
                        "name": "pvc-volume",
                        "persistentVolumeClaim": {
                            "claimName": pvc_name
                        }
                    },
                    {
                        "name": "content-volume",
                        "hostPath": {
                            "path": str(temp_content_file)
                        }
                    }
                ],
                "restartPolicy": "Never"
            }
        }
        
        # Create pod spec file
        temp_file = Path(f"/tmp/{pod_name}.json")
        with open(temp_file, "w") as f:
            json.dump(pod_spec, f)
            
        # Execute operations
        subprocess.run(
            ["kubectl", "create", "-f", str(temp_file), "-n", namespace],
            check=True,
            capture_output=True
        )
        
        # Wait for completion
        start_time = time.time()
        while True:
            status_cmd = ["kubectl", "get", "pod", pod_name, "-n", namespace, "-o", "jsonpath={.status.phase}"]
            result = subprocess.run(status_cmd, check=True, capture_output=True, text=True)
            status = result.stdout.strip()
            
            if status == "Succeeded":
                break
            elif status == "Failed":
                logs_cmd = ["kubectl", "logs", pod_name, "-n", namespace]
                logs = subprocess.run(logs_cmd, check=False, capture_output=True, text=True).stdout
                raise KubernetesPVCError(f"Pod {pod_name} failed. Logs: {logs}")
            
            if time.time() - start_time > timeout:
                raise KubernetesPVCError(f"Timeout waiting for pod {pod_name} to complete")
                
            time.sleep(1)
            
    except subprocess.CalledProcessError as e:
        raise KubernetesPVCError(f"Command failed: {e.cmd}. Error: {e.stderr.decode('utf-8')}")
    except Exception as e:
        raise KubernetesPVCError(f"Failed to create file in PVC: {str(e)}")
    finally:
        # Clean up
        try:
            subprocess.run(
                ["kubectl", "delete", "pod", pod_name, "-n", namespace, "--grace-period=0", "--force"],
                check=False,
                capture_output=True
            )
        except Exception as e:
            logger.warning(f"Failed to clean up pod {pod_name}: {str(e)}")
            
        if temp_file.exists():
            temp_file.unlink()
        if temp_content_file.exists():
            temp_content_file.unlink()

def copy_to_pvc(
    pvc_name: str,
    local_path: str,
    pvc_path: str,
    namespace: str = "default"
) -> None:
    """
    Copy a local file or directory to a PVC.
    
    Args:
        pvc_name: Name of the PVC
        local_path: Local file or directory path
        pvc_path: Destination path in the PVC
        namespace: Kubernetes namespace where the PVC exists
        
    Raises:
        KubernetesPVCError: If the operation fails
    """
    try:
        # Create a temporary pod
        pod_name = f"pvc-copy-{int(time.time())}"
        
        # Create pod with the PVC mounted
        create_cmd = [
            "kubectl", "run", pod_name,
            "--restart=Never",
            "--image=busybox",
            "-n", namespace,
            "--overrides", json.dumps({
                "spec": {
                    "volumes": [{
                        "name": "pvc-volume",
                        "persistentVolumeClaim": {
                            "claimName": pvc_name
                        }
                    }],
                    "containers": [{
                        "name": pod_name,
                        "image": "busybox",
                        "command": ["sleep", "3600"],
                        "volumeMounts": [{
                            "name": "pvc-volume",
                            "mountPath": "/mnt"
                        }]
                    }]
                }
            })
        ]
        
        subprocess.run(create_cmd, check=True, capture_output=True)
        
        # Wait for pod to be ready
        ready_cmd = ["kubectl", "wait", "--for=condition=Ready", f"pod/{pod_name}", "-n", namespace]
        subprocess.run(ready_cmd, check=True, capture_output=True)
        
        # Copy the files
        copy_cmd = ["kubectl", "cp", local_path, f"{namespace}/{pod_name}:/mnt/{pvc_path}"]
        subprocess.run(copy_cmd, check=True, capture_output=True)
        
    except subprocess.CalledProcessError as e:
        raise KubernetesPVCError(f"Command failed: {e.cmd}. Error: {e.stderr.decode('utf-8')}")
    except Exception as e:
        raise KubernetesPVCError(f"Failed to copy to PVC: {str(e)}")
    finally:
        # Clean up
        try:
            subprocess.run(
                ["kubectl", "delete", "pod", pod_name, "-n", namespace, "--grace-period=0", "--force"],
                check=False,
                capture_output=True
            )
        except Exception as e:
            logger.warning(f"Failed to clean up pod {pod_name}: {str(e)}")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Example usage
    try:
        print("Listing contents of lidar-data-pvc:")
        contents = list_pvc_contents("lidar-data-pvc")
        for item in contents:
            print(f"{item['permissions']} {item['owner']:<8} {item['group']:<8} {item['size']:>8} {item['modified']} {item['name']}")
    except KubernetesPVCError as e:
        print(f"Error: {e}")