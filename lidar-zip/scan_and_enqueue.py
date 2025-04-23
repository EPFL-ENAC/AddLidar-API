#!/usr/bin/env python3
# /// script
# dependencies = [
#   "kubernetes",
#   "pydantic",
# ]
# ///
"""
LiDAR Archive Scanner and Job Enqueuer

This script scans directories containing LiDAR data and queues Kubernetes jobs
to create compressed archives of changed directories.
"""

import os
import json
import sqlite3
import subprocess
import time
import uuid
import logging
import sys
from typing import Dict, List, Optional, Tuple

try:
    # Import Kubernetes client - will be automatically installed by uv when running with "uv run"
    from kubernetes import client, config
except ImportError:
    print("Error: kubernetes module not found. Run this script with 'uv run' to auto-install dependencies.")
    sys.exit(1)

# Configure logging
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("lidar-archiver")

# Constants from environment variables with defaults
ORIG = os.environ.get("ORIGINAL_ROOT", "./original_root")
ZIP = os.environ.get("ZIP_ROOT", "./zip_root")
DB = os.environ.get("DB_PATH", "./state/archive.db")

# Configuration validation
if not os.path.isdir(ORIG):
    logger.warning(f"Original root directory '{ORIG}' does not exist, creating it...")
    os.makedirs(ORIG, exist_ok=True)

if not os.path.isdir(ZIP):
    logger.warning(f"Zip root directory '{ZIP}' does not exist, creating it...")
    os.makedirs(ZIP, exist_ok=True)

def fingerprint(path: str) -> str:
    """
    Generate a unique fingerprint for a directory based on file attributes.
    
    Args:
        path: Directory path to fingerprint
        
    Returns:
        SHA-256 hash representing the directory content state
    """
    import hashlib
    import os
    
    try:
        # List to store file information tuples (relative_path, size_bytes, mod_time)
        file_info = []
        
        # Walk through the directory tree
        for root, _, files in os.walk(path):
            for file in files:
                full_path = os.path.join(root, file)
                # Get relative path from the base directory
                rel_path = os.path.relpath(full_path, path)
                
                # Get file stats
                stat_result = os.stat(full_path, follow_symlinks=False)
                size_bytes = stat_result.st_size
                mod_time = stat_result.st_mtime
                
                # Store information as a tuple
                file_info.append((rel_path, size_bytes, mod_time))
        
        # Sort the list to ensure consistent ordering
        file_info.sort()
        
        # Create a hash object
        hasher = hashlib.sha256()
        
        # Add each file's information to the hash
        for rel_path, size_bytes, mod_time in file_info:
            # Format: relative_path|size|modification_time
            file_data = f"{rel_path}|{size_bytes}|{mod_time}\n".encode('utf-8')
            hasher.update(file_data)
        
        # Return the hexadecimal digest
        return hasher.hexdigest()
    except Exception as e:
        logger.error(f"Failed to generate fingerprint for {path}: {e}")
        raise

def queue_zip_job_on_kube(rel_path: str) -> None:
    """
    Create a Kubernetes job to compress a directory into a tar.gz archive.
    
    Args:
        rel_path: Relative path of directory to compress
    """
    full_source = os.path.join(ORIG, rel_path)
    full_dest = os.path.join(ZIP, f"{rel_path}.tar.gz")
    name = f"zip-{uuid.uuid4().hex[:10]}"
    
    try:
        # Create Kubernetes job specification
        job = client.V1Job(
            metadata=client.V1ObjectMeta(name=name),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    spec=client.V1PodSpec(
                        restart_policy="Never",
                        containers=[client.V1Container(
                            name="zip",
                            image="alpine:3.18",  # Updated Alpine version for stability
                            command=["/bin/sh", "-c",
                                f"mkdir -p $(dirname '{full_dest}') && "
                                f"tar -C '{ORIG}/{os.path.dirname(rel_path)}' "
                                f"--use-compress-program=pigz -cf '{full_dest}' "
                                f"'{os.path.basename(rel_path)}'"
                            ],
                            volume_mounts=[
                                client.V1VolumeMount(mount_path=ORIG, name="orig", read_only=True),
                                client.V1VolumeMount(mount_path=ZIP, name="zip"),
                            ]
                        )],
                        volumes=[
                            client.V1Volume(name="orig", host_path=client.V1HostPathVolumeSource(path=ORIG)),
                            client.V1Volume(name="zip", host_path=client.V1HostPathVolumeSource(path=ZIP)),
                        ]
                    )
                )
            )
        )
        
        # Submit job to Kubernetes
        client.BatchV1Api().create_namespaced_job("archives", job)
        logger.info(f"Queued archive job '{name}' for directory: {rel_path}")
    except Exception as e:
        logger.error(f"Failed to create K8s job for {rel_path}: {e}")
        raise


def queue_zip_job_on_docker(rel_path: str) -> None:
    """
    Create a Docker container to compress a directory into a tar.gz archive.
    
    Args:
        rel_path: Relative path of directory to compress
    """
    full_source = os.path.join(ORIG, rel_path)
    full_dest = os.path.join(ZIP, f"{rel_path}.tar.gz")
    name = f"zip-{uuid.uuid4().hex[:10]}"
    
    try:
        # Create directory for the archive if it doesn't exist
        os.makedirs(os.path.dirname(full_dest), exist_ok=True)
        
        # Build Docker run command
        docker_cmd = [
            "docker", "run", "--rm",
            "--name", name,
            "-v", f"{ORIG}:{ORIG}:ro",
            "-v", f"{ZIP}:{ZIP}",
            "alpine:3.18",
            "/bin/sh", "-c",
            f"apk add --no-cache pigz && "
            f"mkdir -p $(dirname '{full_dest}') && "
            f"tar -C '{ORIG}/{os.path.dirname(rel_path)}' "
            f"--use-compress-program=pigz -cf '{full_dest}' "
            f"'{os.path.basename(rel_path)}'"
        ]
        
        # Start Docker container in background
        process = subprocess.Popen(
            docker_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        logger.info(f"Launched Docker container '{name}' for directory: {rel_path}")
    except Exception as e:
        logger.error(f"Failed to create Docker job for {rel_path}: {e}")
        raise


def queue_zip_job_on_local(rel_path: str) -> None:
    """
    Run a local process to compress a directory into a tar.gz archive.
    
    Args:
        rel_path: Relative path of directory to compress
    """
    full_source = os.path.join(ORIG, rel_path)
    full_dest = os.path.join(ZIP, f"{rel_path}.tar.gz")
    
    try:
        # Create directory for the archive if it doesn't exist
        os.makedirs(os.path.dirname(full_dest), exist_ok=True)
        
        # Check if pigz is available, use gzip as fallback
        try:
            subprocess.run(["which", "pigz"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            compress_program = "pigz"
        except subprocess.CalledProcessError:
            logger.warning("pigz not found, falling back to gzip")
            compress_program = "gzip"
        
        # Build tar command
        tar_cmd = [
            "tar",
            "-C", f"{ORIG}/{os.path.dirname(rel_path)}",
            f"--use-compress-program={compress_program}",
            "-cf", full_dest,
            os.path.basename(rel_path)
        ]
        
        # Execute the tar command in background
        process = subprocess.Popen(
            tar_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        logger.info(f"Started local compression process for directory: {rel_path}")
    except Exception as e:
        logger.error(f"Failed to create local job for {rel_path}: {e}")
        raise


def queue_zip_job(rel_path: str) -> None:
    """
    Queue a job to compress a directory based on configured execution environment.
    
    Args:
        rel_path: Relative path of directory to compress
    """
    # Get execution environment from environment variable, default to local
    execution_env = os.environ.get("EXECUTION_ENV", "local").lower()
    
    if execution_env == "kubernetes" or execution_env == "kube":
        queue_zip_job_on_kube(rel_path)
    elif execution_env == "docker":
        queue_zip_job_on_docker(rel_path)
    else:  # Default to local
        queue_zip_job_on_local(rel_path)

def get_directory_stats(path: str) -> Tuple[str, int, int]:
    """
    Get directory statistics: fingerprint, size in KB, and file count.
    
    Args:
        path: Path to directory
        
    Returns:
        Tuple containing (fingerprint, size_kb, file_count)
    """
    fp = fingerprint(path)
    try:
        size = int(subprocess.check_output(["du", "-sk", path]).split()[0])
        count = int(subprocess.check_output(["bash", "-c", f"find '{path}' -type f | wc -l"]).strip())
        return fp, size, count
    except subprocess.SubprocessError as e:
        logger.error(f"Failed to get stats for directory {path}: {e}")
        raise

def init_database(db_path: str) -> sqlite3.Connection:
    """
    Initialize the SQLite database connection and schema.
    
    Args:
        db_path: Path to SQLite database file
        
    Returns:
        Initialized database connection
    """
    # Ensure directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Create and connect to the database
    db = sqlite3.connect(db_path)
    db.execute("PRAGMA journal_mode=WAL")
    
    # Read and execute the schema from persist_state.sql
    sql_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "persist_state.sql")
    try:
        with open(sql_file_path, 'r') as f:
            schema_sql = f.read()
        db.executescript(schema_sql)
        logger.info(f"Initialized database schema from {sql_file_path}")
    except Exception as e:
        logger.error(f"Failed to initialize database schema from file: {e}")
        # Fall back to the inline schema as a backup
        db.execute("""CREATE TABLE IF NOT EXISTS folder_state (
                    folder_key TEXT PRIMARY KEY, fp TEXT,
                    size_kb INT, file_count INT,
                    last_seen INT, archived_at INT, zip_path TEXT)""")
        logger.warning("Using fallback inline schema definition")
    
    return db

def main() -> None:
    """
    Main function to scan directories and enqueue archive jobs.
    """
    try:
        # Load Kubernetes configuration
        config.load_incluster_config()
        logger.info("Loaded Kubernetes in-cluster config")
    except Exception as e:
        logger.warning(f"Failed to load in-cluster config, trying local: {e}")
        try:
            config.load_kube_config()
            logger.info("Loaded local Kubernetes config")
        except Exception as e:
            logger.error(f"Failed to load any Kubernetes config: {e}")
            raise
    
    # Initialize database
    db = init_database(DB)
    logger.info(f"Initialized database at {DB}")
    
    try:
        processed_count = 0
        changed_count = 0
        
        # Scan level1/level2 directories
        for level1 in os.listdir(ORIG):
            p1 = os.path.join(ORIG, level1)
            if not os.path.isdir(p1):
                continue
                
            for level2 in os.listdir(p1):
                rel = os.path.join(level1, level2)
                src = os.path.join(ORIG, rel)
                if not os.path.isdir(src):
                    continue

                processed_count += 1
                
                # Get directory statistics
                try:
                    fp, size, count = get_directory_stats(src)
                    
                    # Check if directory changed since last scan
                    row = db.execute("SELECT fp FROM folder_state WHERE folder_key=?", (rel,)).fetchone()
                    if not row or row[0] != fp:
                        # Directory is new or changed - enqueue job
                        changed_count += 1
                        logger.info(f"Change detected in {rel}, enqueueing archive job")
                        queue_zip_job(rel)
                        
                        # Update database record
                        db.execute("""INSERT INTO folder_state
                                    (folder_key, fp, size_kb, file_count, last_seen, archived_at, zip_path)
                                    VALUES (?, ?, ?, ?, ?, NULL, ?)
                                    ON CONFLICT(folder_key) DO UPDATE SET
                                    fp = excluded.fp,
                                    size_kb = excluded.size_kb,
                                    file_count = excluded.file_count,
                                    last_seen = excluded.last_seen,
                                    archived_at = NULL""",
                                (rel, fp, size, count, int(time.time()),
                                os.path.join(ZIP, f"{rel}.tar.gz")))
                except Exception as e:
                    logger.error(f"Error processing directory {rel}: {e}")
        
        # Commit all database changes
        db.commit()
        logger.info(f"Scan completed: processed {processed_count} directories, detected {changed_count} changes")
    except Exception as e:
        logger.error(f"Error during directory scan: {e}")
        db.rollback()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    main()
