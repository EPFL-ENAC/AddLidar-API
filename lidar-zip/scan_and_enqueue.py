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
        # Create Kubernetes job specification with a post-completion script to update the database
        archive_script = (
            f"mkdir -p $(dirname '{full_dest}') && "
            f"tar -C '{ORIG}/{os.path.dirname(rel_path)}' "
            f"--use-compress-program=pigz -cf '{full_dest}' "
            f"'{os.path.basename(rel_path)}' && "
            f"echo 'Archive created successfully: {full_dest}' && "
            # Add database update command after successful archive creation
            f"python3 -c \"import sqlite3, time; "
            f"db = sqlite3.connect('{DB}'); "
            f"db.execute('UPDATE folder_state SET archived_at = ? WHERE folder_key = ?', "
            f"(int(time.time()), '{rel_path}')); "
            f"db.commit(); "
            f"db.close(); "
            f"print('Database updated for {rel_path}')\" || "
            f"echo 'Failed to update database for {rel_path}'"
        )
        
        job = client.V1Job(
            metadata=client.V1ObjectMeta(name=name),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    spec=client.V1PodSpec(
                        restart_policy="Never",
                        containers=[client.V1Container(
                            name="zip",
                            image="python:3.9-alpine",  # Using Python image to enable database update
                            command=["/bin/sh", "-c", archive_script],
                            volume_mounts=[
                                client.V1VolumeMount(mount_path=ORIG, name="orig", read_only=True),
                                client.V1VolumeMount(mount_path=ZIP, name="zip"),
                                client.V1VolumeMount(mount_path=os.path.dirname(DB), name="db"),
                            ]
                        )],
                        volumes=[
                            client.V1Volume(name="orig", host_path=client.V1HostPathVolumeSource(path=ORIG)),
                            client.V1Volume(name="zip", host_path=client.V1HostPathVolumeSource(path=ZIP)),
                            client.V1Volume(name="db", host_path=client.V1HostPathVolumeSource(path=os.path.dirname(DB))),
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
        
        # Build Docker run command with database update after successful archive
        docker_cmd = [
            "docker", "run", "--rm",
            "--name", name,
            "-v", f"{ORIG}:{ORIG}:ro",
            "-v", f"{ZIP}:{ZIP}",
            "-v", f"{os.path.dirname(DB)}:{os.path.dirname(DB)}",
            "python:3.9-alpine",
            "/bin/sh", "-c",
            f"apk add --no-cache pigz sqlite && "
            f"mkdir -p $(dirname '{full_dest}') && "
            f"tar -C '{ORIG}/{os.path.dirname(rel_path)}' "
            f"--use-compress-program=pigz -cf '{full_dest}' "
            f"'{os.path.basename(rel_path)}' && "
            f"echo 'Archive created successfully: {full_dest}' && "
            # Add database update command after successful archive creation
            f"python3 -c \"import sqlite3, time; "
            f"db = sqlite3.connect('{DB}'); "
            f"db.execute('UPDATE folder_state SET archived_at = ? WHERE folder_key = ?', "
            f"(int(time.time()), '{rel_path}')); "
            f"db.commit(); "
            f"db.close(); "
            f"print('Database updated for {rel_path}')\" || "
            f"echo 'Failed to update database for {rel_path}'"
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
        
        # Execute the tar command and wait for it to complete
        try:
            result = subprocess.run(
                tar_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True
            )
            
            logger.info(f"Completed local compression for directory: {rel_path}")
            
            # Update the database after successful archive creation
            # Use a completely separate connection that will be properly closed
            db_manager = DatabaseManager(DB)
            try:
                with db_manager.get_connection() as conn:
                    # Update the database with archive time
                    conn.execute(
                        "UPDATE folder_state SET archived_at = ? WHERE folder_key = ?",
                        (int(time.time()), rel_path)
                    )
                    conn.commit()
                    
                    # Check status in database and log it
                    cursor = conn.execute("SELECT fp, size_kb, file_count, last_seen, archived_at, zip_path FROM folder_state WHERE folder_key = ?", (rel_path,))
                    record = cursor.fetchone()
                    if record:
                        fp, size_kb, file_count, last_seen, archived_at, zip_path = record
                        logger.info(f"Database record for {rel_path}: fp={fp[:8]}..., size={size_kb}KB, files={file_count}, archived_at={archived_at}, zip_path={zip_path}")
                    else:
                        logger.warning(f"No database record found for {rel_path} after update")
                    
                logger.info(f"Database updated for {rel_path}")
            except sqlite3.Error as db_error:
                logger.error(f"Failed to update database for {rel_path}: {db_error}")
                
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to create archive for {rel_path}: {e.stderr.decode('utf-8', errors='replace')}")
            
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
    execution_env: str = os.environ.get("EXECUTION_ENV", "local").lower()
    logger.info(f"Queueing job for {rel_path} using environment: {execution_env}")

    if execution_env in ("kubernetes", "kube"):
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

class DatabaseManager:
    """
    Manages SQLite database connections with connection pooling and proper resource management.
    Follows context manager protocol for safe resource handling.
    """
    
    _schema_initialized = False
    
    def __init__(self, db_path: str):
        """
        Initialize the database manager.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
    @classmethod
    def init_schema(cls, db_path: str) -> None:
        """Initialize the database schema if not already done.
        
        This is a class method that should be called once during application startup.
        
        Args:
            db_path: Path to SQLite database file
        """
        if cls._schema_initialized:
            return
            
        # Create a temporary connection just for schema initialization
        conn = sqlite3.connect(db_path, timeout=20.0)
        try:
            conn.execute("PRAGMA busy_timeout = 10000")  # 10 seconds
            conn.execute("PRAGMA journal_mode=WAL")
            
            # Read and execute the schema from persist_state.sql
            sql_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "persist_state.sql")
            try:
                with open(sql_file_path, 'r') as f:
                    schema_sql = f.read()
                conn.executescript(schema_sql)
                logger.info(f"Initialized database schema from {sql_file_path}")
            except Exception as e:
                logger.error(f"Failed to initialize database schema from file: {e}")
                # Fall back to the inline schema as a backup
                conn.execute("""CREATE TABLE IF NOT EXISTS folder_state (
                            folder_key TEXT PRIMARY KEY, fp TEXT,
                            size_kb INT, file_count INT,
                            last_seen INT, archived_at INT, zip_path TEXT)""")
                
                conn.execute("""CREATE INDEX IF NOT EXISTS idx_folder_key ON folder_state (folder_key)""")
                logger.warning("Using fallback inline schema definition")
            
            cls._schema_initialized = True
        finally:
            conn.close()
    
    def get_connection(self) -> sqlite3.Connection:
        """
        Get a new database connection with optimized settings.
        
        Returns:
            A new SQLite connection
        """
        conn = sqlite3.connect(self.db_path, timeout=20.0)
        conn.execute("PRAGMA busy_timeout = 10000")  # 10 seconds
        return conn


def init_database(db_path: str) -> DatabaseManager:
    """
    Initialize the SQLite database manager.
    
    Args:
        db_path: Path to SQLite database file
        
    Returns:
        Initialized database manager
    """
    # Initialize the schema once at startup
    DatabaseManager.init_schema(db_path)
    # Return a manager instance
    return DatabaseManager(db_path)

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
    
    # Initialize database manager
    db_manager = init_database(DB)
    logger.info(f"Initialized database at {DB}")
    
    processed_count = 0
    changed_count = 0
    
    try:
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
                    
                    # Use a fresh connection for each operation to prevent locking
                    with db_manager.get_connection() as conn:
                        # Check if directory changed since last scan
                        cursor = conn.execute("SELECT fp FROM folder_state WHERE folder_key=?", (rel,))
                        row = cursor.fetchone()
                        
                        if not row or row[0] != fp:
                            # Directory is new or changed - enqueue job
                            changed_count += 1
                            logger.info(f"Change detected in {rel}, enqueueing archive job")
                            
                            # Insert the new record or update the change status first
                            conn.execute(
                                """INSERT INTO folder_state
                                (folder_key, fp, size_kb, file_count, last_seen, archived_at, zip_path)
                                VALUES (?, ?, ?, ?, ?, NULL, ?)
                                ON CONFLICT(folder_key) DO UPDATE SET
                                fp = excluded.fp,
                                size_kb = excluded.size_kb,
                                file_count = excluded.file_count,
                                last_seen = excluded.last_seen,
                                archived_at = NULL""",
                                (rel, fp, size, count, int(time.time()),
                                 os.path.join(ZIP, f"{rel}.tar.gz"))
                            )
                            conn.commit()
                            
                            # Now that the database record exists, queue the job
                            queue_zip_job(rel)
                except Exception as e:
                    logger.error(f"Error processing directory {rel}: {e}")
        
        logger.info(f"Scan completed: processed {processed_count} directories, detected {changed_count} changes")
    except Exception as e:
        logger.error(f"Error during directory scan: {e}")
        raise

if __name__ == "__main__":
    main()
