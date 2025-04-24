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
import argparse
from typing import Dict, List, Optional, Tuple

try:
    # Import Kubernetes client - will be automatically installed by uv when running with "uv run"
    from kubernetes import client, config
except ImportError:
    print("Error: kubernetes module not found. Run this script with 'uv run' to auto-install dependencies.")
    sys.exit(1)

# Initial logger setup with default level (will be updated in main)
logging.basicConfig(
    level=logging.INFO,  # Default level, will be overridden in main()
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("lidar-archiver")

# Constants will be set in main() from arguments
ORIG: str = ""
ZIP: str = ""
DB: str = ""
# We'll store parsed args globally so they can be accessed from other functions
args = None


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

def queue_zip_job_on_kube(rel_path: str, export_only: bool = False) -> None:
    # Access global constants ORIG, ZIP, DB
    global ORIG, ZIP, DB
    full_source = os.path.join(ORIG, rel_path)
    full_dest = os.path.join(ZIP, f"{rel_path}.tar.gz")
    name = f"zip-{uuid.uuid4().hex[:10]}"
    try:
        archive_script = (
            f"mkdir -p $(dirname '{full_dest}') && "
            f"tar -C '{ORIG}/{os.path.dirname(rel_path)}' "
            f"--use-compress-program=pigz -cf '{full_dest}' "
            f"'{os.path.basename(rel_path)}' && "
            f"echo 'Archive created successfully: {full_dest}' && "
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
                            image="python:3.9-alpine",
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
        if export_only:
            from kubernetes.client import ApiClient
            job_yaml = ApiClient().sanitize_for_serialization(job)
            import yaml
            print(yaml.dump(job_yaml))
            logger.info(f"Exported Kubernetes job YAML for directory: {rel_path}")
        else:
            client.BatchV1Api().create_namespaced_job("archives", job)
            logger.info(f"Queued archive job '{name}' for directory: {rel_path}")
    except Exception as e:
        logger.error(f"Failed to create K8s job for {rel_path}: {e}")
        raise


def queue_zip_job_on_docker(rel_path: str, export_only: bool = False) -> None:
    # Access global constants ORIG, ZIP, DB
    global ORIG, ZIP, DB
    full_source = os.path.join(ORIG, rel_path)
    full_dest = os.path.join(ZIP, f"{rel_path}.tar.gz")
    name = f"zip-{uuid.uuid4().hex[:10]}"
    try:
        os.makedirs(os.path.dirname(full_dest), exist_ok=True)
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
            f"python3 -c \"import sqlite3, time; "
            f"db = sqlite3.connect('{DB}'); "
            f"db.execute('UPDATE folder_state SET archived_at = ? WHERE folder_key = ?', "
            f"(int(time.time()), '{rel_path}')); "
            f"db.commit(); "
            f"db.close(); "
            f"print('Database updated for {rel_path}')\" || "
            f"echo 'Failed to update database for {rel_path}'"
        ]
        
        if export_only:
            print("Docker command:", " ".join(docker_cmd))
            logger.info(f"Exported Docker command for directory: {rel_path}")
        else:
            process = subprocess.Popen(
                docker_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            logger.info(f"Launched Docker container '{name}' for directory: {rel_path}")
    except Exception as e:
        logger.error(f"Failed to create Docker job for {rel_path}: {e}")
        raise


def queue_zip_job_on_local(rel_path: str, export_only: bool = False) -> None:
    # Access global constants ORIG, ZIP, DB
    global ORIG, ZIP, DB
    full_source = os.path.join(ORIG, rel_path)
    full_dest = os.path.join(ZIP, f"{rel_path}.tar.gz")
    try:
        os.makedirs(os.path.dirname(full_dest), exist_ok=True)
        try:
            subprocess.run(["which", "pigz"], check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            compress_program = "pigz"
        except subprocess.CalledProcessError:
            logger.warning("pigz not found, falling back to gzip")
            compress_program = "gzip"
        tar_cmd = [
            "tar",
            "-C", f"{ORIG}/{os.path.dirname(rel_path)}",
            f"--use-compress-program={compress_program}",
            "-cf", full_dest,
            os.path.basename(rel_path)
        ]
        if export_only:
            print("Tar command:", " ".join(tar_cmd))
            logger.info(f"Exported local tar command for directory: {rel_path}")
            return

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
            # Pass DB path explicitly
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
        
    logger.info(f"Completed local compression for directory: {rel_path}")

def queue_zip_job(rel_path: str, export_only: bool = False) -> None:
    """
    Queue a job to compress a directory based on the configured execution environment.
    """
    # Use the execution_env from command-line arguments instead of environment variable
    global args
    execution_env: str = args.execution_env.lower()
    
    logger.info(f"Queueing job for {rel_path} using environment: {execution_env}")
    if execution_env in ("kubernetes", "kube"):
        queue_zip_job_on_kube(rel_path, export_only)
    elif execution_env == "docker":
        queue_zip_job_on_docker(rel_path, export_only)
    else:
        queue_zip_job_on_local(rel_path, export_only)


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
        """Initialize the database schema if not already done."""
        if cls._schema_initialized:
            return
        
        # Ensure directory exists before trying to connect
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
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
    # Access global constants and args to modify them
    global ORIG, ZIP, DB, args

    parser = argparse.ArgumentParser(
        description="LiDAR Archive Scanner and Job Enqueuer"
    )
    parser.add_argument(
        "--original-root",
        default="./original_root",
        help="Root directory containing original LiDAR data"
    )
    parser.add_argument(
        "--zip-root",
        default="./zip_root",
        help="Root directory where compressed archives will be stored"
    )
    parser.add_argument(
        "--db-path",
        default="./state/archive.db",
        help="Path to the SQLite database file for tracking state"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set logging level (default: INFO)"
    )
    parser.add_argument(
        "--execution-env",
        choices=["local", "docker", "kubernetes", "kube"],
        default="local",
        help="Execution environment for archive jobs (default: local)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check for changes without modifying database or queueing jobs"
    )
    parser.add_argument(
        "--export-only",
        action="store_true",
        help="Print job YAMLs/commands instead of creating them"
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=0,
        help="Stop after the specified number of archive jobs have been queued (0 for unlimited)"
    )
    args = parser.parse_args()

    # Set logging level from command line argument
    log_level = args.log_level.upper()
    logger.setLevel(getattr(logging, log_level))
    logger.info(f"Log level set to: {log_level}")

    # Assign parsed arguments to global constants
    ORIG = args.original_root
    ZIP = args.zip_root
    DB = args.db_path
    execution_env = args.execution_env.lower()

    dry_run: bool = args.dry_run
    export_only: bool = args.export_only
    max_jobs: Optional[int] = args.max_jobs if args.max_jobs > 0 else None

    # Configuration validation moved here
    if not os.path.isdir(ORIG):
        logger.warning(f"Original root directory '{ORIG}' does not exist, creating it...")
        os.makedirs(ORIG, exist_ok=True)

    if not os.path.isdir(ZIP):
        logger.warning(f"Zip root directory '{ZIP}' does not exist, creating it...")
        os.makedirs(ZIP, exist_ok=True)

    # Ensure DB directory exists
    db_dir = os.path.dirname(DB)
    if db_dir and not os.path.isdir(db_dir):
         logger.warning(f"Database directory '{db_dir}' does not exist, creating it...")
         os.makedirs(db_dir, exist_ok=True)

    logger.info(f"Starting scan: ORIG='{ORIG}', ZIP='{ZIP}', DB='{DB}'")
    logger.info(f"Options: execution_env='{execution_env}', log_level='{log_level}', "
                f"dry-run={dry_run}, export_only={export_only}, max_jobs={max_jobs}")

    # Load Kube config only if needed - now using the command line argument
    if execution_env in ("kubernetes", "kube"):
        try:
            config.load_incluster_config()
            logger.info("Loaded Kubernetes in-cluster config")
        except config.ConfigException:
            logger.warning("Failed to load in-cluster config, trying local kube config")
            try:
                config.load_kube_config()
                logger.info("Loaded local Kubernetes config")
            except Exception as e:
                logger.error(f"Failed to load any Kubernetes config: {e}")
                # Exit if Kubernetes mode is requested but config loading fails
                sys.exit(1)

    logger.info(f"Initializing database... at path ${DB}")
    db_manager = init_database(DB) # DB path comes from args now
    logger.info(f"Initialized database at {DB}")

    processed_count = 0
    changed_count = 0
    jobs_queued = 0
    max_jobs_reached = False

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
            try:
                logger.info(f"Processing directory: {rel}")
                fp, size, count = get_directory_stats(src)
                if not dry_run:
                    with db_manager.get_connection() as conn:
                        cursor = conn.execute("SELECT fp FROM folder_state WHERE folder_key=?", (rel,))
                        row = cursor.fetchone()
                        if not row or row[0] != fp:
                            changed_count += 1
                            logger.info(f"Change detected in {rel}, enqueueing archive job")
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
                            queue_zip_job(rel, export_only)
                            jobs_queued += 1
                            if max_jobs is not None and jobs_queued >= max_jobs:
                                logger.info("Reached maximum number of archive jobs to run. Stopping scan.")
                                max_jobs_reached = True
                                break
                else:
                    # In dry-run mode, just log the detected change
                    if not os.path.isdir(src):
                        continue
                    with db_manager.get_connection() as conn:
                        cursor = conn.execute("SELECT fp FROM folder_state WHERE folder_key=?", (rel,))
                        row = cursor.fetchone()
                    if not row or row[0] != fp:
                        changed_count += 1
                        jobs_queued += 1
                        if max_jobs is not None and jobs_queued >= max_jobs:
                            logger.info("Reached maximum number of archive jobs to run. Stopping scan.")
                            max_jobs_reached = True
                            break
                        logger.info(f"[Dry-run] Change detected in {rel}, no DB update or job queuing.")

            except Exception as e:
                logger.error(f"Error processing directory {rel}: {e}")
        if max_jobs_reached:
            break

    logger.info(f"Scan completed: processed {processed_count} directories, detected {changed_count} changes")

if __name__ == "__main__":
    main()
