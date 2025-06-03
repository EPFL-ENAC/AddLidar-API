#!/usr/bin/env python3
# /// script
# dependencies = [
#   "kubernetes",
#   "pydantic",
#   "jinja2",
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
from typing import Dict, List, Optional, Tuple, Set
from datetime import datetime

try:
    # Import dependencies
    from kubernetes import client, config
    import jinja2
except ImportError:
    print(
        "Error: required modules not found. Run this script with 'uv run' to auto-install dependencies."
    )
    sys.exit(1)

# Initial logger setup with default level (will be updated in main)
logging.basicConfig(
    level=logging.INFO,  # Default level, will be overridden in main()
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("lidar-archiver")

# Constants will be set in main() from arguments
ORIG: str = ""
ZIP: str = ""
DB: str = ""
# We'll store parsed args globally so they can be accessed from other functions
args = None


def fingerprint_file(file_path: str) -> str:
    """
    Generate a unique fingerprint for a single file.

    Args:
        file_path: Path to the file

    Returns:
        SHA-256 hash representing the file content
    """
    import hashlib

    try:
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            # Read in chunks for memory efficiency
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception as e:
        logger.error(f"Failed to generate fingerprint for file {file_path}: {e}")
        raise


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
            file_data = f"{rel_path}|{size_bytes}|{mod_time}\n".encode("utf-8")
            hasher.update(file_data)

        # Return the hexadecimal digest
        return hasher.hexdigest()
    except Exception as e:
        logger.error(f"Failed to generate fingerprint for {path}: {e}")
        raise


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
        count = int(
            subprocess.check_output(
                ["bash", "-c", f"find '{path}' -type f | wc -l"]
            ).strip()
        )
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
            sql_file_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "persist_state.sql"
            )
            try:
                with open(sql_file_path, "r") as f:
                    schema_sql = f.read()
                conn.executescript(schema_sql)
                logger.info(f"Initialized database schema from {sql_file_path}")
            except Exception as e:
                logger.error(f"Failed to initialize database schema from file: {e}")
                # Fall back to the inline schema as a backup
                conn.execute(
                    """CREATE TABLE IF NOT EXISTS folder_state (
                            folder_key TEXT PRIMARY KEY, fp TEXT,
                            size_kb INT, file_count INT,
                            last_seen INT, archived_at INT, zip_path TEXT)"""
                )

                conn.execute(
                    """CREATE INDEX IF NOT EXISTS idx_folder_key ON folder_state (folder_key)"""
                )
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


def scan_for_metacloud_files(
    db_manager: DatabaseManager, dry_run: bool = False
) -> List[List[str]]:
    """
    Scan directories for .metacloud files and track changes.

    Args:
        db_manager: Database manager instance
        dry_run: Whether to perform a dry run without modifying the database

    Returns:
        List of lists containing [mission_key, metacloud_path, fingerprint] that have changed
    """
    global ORIG
    metacloud_changes: List[List[str]] = []
    current_time = int(time.time())

    # First, list all level1 directories (missions)
    for level1 in os.listdir(ORIG):
        p1 = os.path.join(ORIG, level1)
        if not os.path.isdir(p1):
            continue

        # Look for .metacloud file in the mission directory
        metacloud_file = None
        for file in os.listdir(p1):
            if file.endswith(".metacloud"):
                metacloud_file = os.path.join(p1, file)
                break

        if not metacloud_file:
            logger.info(f"No .metacloud file found in mission {level1}")
            continue

        # Get fingerprint of the metacloud file
        try:
            metacloud_fp = fingerprint_file(metacloud_file)
            logger.info(
                f"Found .metacloud file in {level1}, fingerprint: {metacloud_fp}"
            )

            # Check if we have this mission key in folder_state
            with db_manager.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT 1 FROM folder_state WHERE mission_key=? LIMIT 1", (level1,)
                )
                if not cursor.fetchone():
                    logger.info(
                        f"Mission {level1} not in folder_state, skipping metacloud processing"
                    )
                    continue

            # Check if the metacloud file has changed or needs reprocessing
            with db_manager.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT fp, processing_status FROM potree_metacloud_state WHERE mission_key=?",
                    (level1,),
                )
                row = cursor.fetchone()

            # Check if metacloud file needs processing:
            # 1. New file (not in database)
            # 2. Fingerprint has changed
            # 3. Previous processing failed or is still pending
            needs_processing = False
            if not row:
                logger.info(f"New .metacloud file detected for mission {level1}")
                needs_processing = True
            elif row[0] != metacloud_fp:
                logger.info(
                    f"Fingerprint change detected in .metacloud file for mission {level1}"
                )
                needs_processing = True
            elif row[1] in ("pending", "failed", None):
                logger.info(
                    f"Incomplete processing detected for .metacloud file in mission {level1} (status: {row[1]})"
                )
                needs_processing = True

            if needs_processing:
                logger.info(
                    f"Adding .metacloud file for mission {level1} to processing queue"
                )
                metacloud_changes.append([level1, metacloud_file, metacloud_fp])

                if not dry_run:
                    output_path = os.path.join(os.path.dirname(ZIP), "Potree", level1)

                    with db_manager.get_connection() as conn:
                        conn.execute(
                            """INSERT INTO potree_metacloud_state
                            (mission_key, fp, output_path, last_checked, last_processed, processing_status)
                            VALUES (?, ?, ?, ?, NULL, 'pending')
                            ON CONFLICT(mission_key) DO UPDATE SET
                            fp = excluded.fp,
                            output_path = excluded.output_path,
                            last_checked = excluded.last_checked,
                            last_processed = NULL,
                            processing_status = 'pending'""",
                            (
                                level1,
                                metacloud_fp,
                                output_path,
                                current_time,
                            ),
                        )
                        conn.commit()
            else:
                # Just update the last_checked timestamp for successful completions
                if not dry_run:
                    with db_manager.get_connection() as conn:
                        conn.execute(
                            """UPDATE potree_metacloud_state
                            SET last_checked = ?
                            WHERE mission_key = ?""",
                            (current_time, level1),
                        )
                        conn.commit()
                logger.debug(
                    f"No processing needed for .metacloud file in mission {level1} (status: {row[1] if row else 'N/A'})"
                )

        except Exception as e:
            logger.error(f"Error processing metacloud file in {level1}: {e}")

    return metacloud_changes


def collect_changed_folders(
    db_manager: DatabaseManager, dry_run: bool = False
) -> List[List[str]]:
    """
    Scan directories and collect paths of changed folders without immediately queueing jobs.

    Args:
        db_manager: Database manager instance
        dry_run: Whether to perform a dry run without modifying the database

    Returns:
        List of relative paths to folders that have changed
    """
    global ORIG
    changed_folders: List[List[str]] = []

    for level1 in os.listdir(ORIG):
        p1 = os.path.join(ORIG, level1)
        if not os.path.isdir(p1):
            continue

        for level2 in os.listdir(p1):
            rel = os.path.join(level1, level2)
            src = os.path.join(ORIG, rel)
            if not os.path.isdir(src):
                continue

            try:
                logger.info(f"Processing directory: {rel}")
                fp, size, count = get_directory_stats(src)
                logger.info(f"Fingerprint: {fp}, Size: {size} KB, File Count: {count}")

                with db_manager.get_connection() as conn:
                    cursor = conn.execute(
                        "SELECT fp, processing_status FROM folder_state WHERE folder_key=?",
                        (rel,),
                    )
                    row = cursor.fetchone()

                # Check if folder needs processing:
                # 1. New folder (not in database)
                # 2. Fingerprint has changed
                # 3. Previous processing failed or is still pending
                needs_processing = False
                if not row:
                    logger.info(f"New folder detected: {rel}")
                    needs_processing = True
                elif row[0] != fp:
                    logger.info(f"Fingerprint change detected in {rel}")
                    needs_processing = True
                elif row[1] in ("pending", "failed", None):
                    logger.info(
                        f"Incomplete processing detected in {rel} (status: {row[1]})"
                    )
                    needs_processing = True

                if needs_processing:
                    logger.info(f"Adding {rel} to processing queue")
                    changed_folders.append([rel, fp])

                    if not dry_run:
                        with db_manager.get_connection() as conn:
                            conn.execute(
                                """INSERT INTO folder_state
                            (folder_key, mission_key, fp, size_kb, file_count, last_checked, last_processed, processing_status, output_path)
                            VALUES (?, ?, ?, ?, ?, ?, NULL, 'pending', ?)
                            ON CONFLICT(folder_key) DO UPDATE SET
                            mission_key = excluded.mission_key,
                            fp = excluded.fp,
                            size_kb = excluded.size_kb,
                            file_count = excluded.file_count,
                            last_checked = excluded.last_checked,
                            last_processed = NULL,
                            processing_status = 'pending',
                            output_path = excluded.output_path""",
                                (
                                    rel,
                                    level1,
                                    fp,
                                    size,
                                    count,
                                    int(time.time()),
                                    os.path.join(ZIP, f"{rel}.tar.gz"),
                                ),
                            )
                            conn.commit()
                else:
                    logger.debug(
                        f"No processing needed for {rel} (status: {row[1] if row else 'N/A'})"
                    )

            except Exception as e:
                logger.error(f"Error processing directory {rel}: {e}")

    return changed_folders


def queue_potree_conversion_jobs(
    metacloud_files: List[List[str]], export_only: bool = False
) -> Optional[int]:
    """
    Create a Kubernetes batch job for Potree conversion of metacloud files using a template.

    Args:
        metacloud_files: List containing [mission_key, metacloud_path, fingerprint] lists
        export_only: Whether to only export the job YAML without creating it

    Returns:
        Optional[int]: Number of jobs created (1 if batch job created) or None if no action was taken
    """
    global ORIG, ZIP, DB, args

    if not metacloud_files:
        logger.info("No metacloud files to process, skipping job creation")
        return None

    try:
        # Load Jinja2 template
        template_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "job-batch-potree-converter.template.yaml",
        )

        if not os.path.exists(template_path):
            logger.error(f"Potree template file not found at {template_path}")
            return None

        with open(template_path, "r") as f:
            template_content = f.read()

        # Setup Jinja2 environment
        template = jinja2.Template(template_content)

        # Generate timestamp for unique job name
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        # Prepare template context
        parallelism = min(
            len(metacloud_files), 4
        )  # Limit parallelism based on number of files

        context = {
            "timestamp": timestamp,
            "metacloud_files": metacloud_files,
            "parallelism": parallelism,
            "db_path": DB,
            "db_dir": os.path.dirname(DB),
        }

        # Render the template
        job_yaml = template.render(**context)

        if export_only:
            print(job_yaml)
            logger.info(
                f"Printed batch Potree job YAML for {len(metacloud_files)} metacloud files"
            )
            return 1

        # Create job from YAML
        import yaml
        from kubernetes import utils

        job_dict = yaml.safe_load(job_yaml)
        try:
            result = utils.create_from_dict(client.ApiClient(), job_dict, True)
            job_name = job_dict["metadata"]["name"]
            logger.info(
                f"Created batch Potree conversion job '{job_name}' for {len(metacloud_files)} metacloud files"
            )
            logger.debug(f"Job creation result: {result}")
            return 1
        except Exception as api_ex:
            logger.error(f"Failed to create Kubernetes batch job via API: {api_ex}")
            return None

    except Exception as e:
        logger.error(f"Failed to create Potree conversion batch job: {e}")
        return None


def queue_batch_zip_job(
    folders: List[List[str]], export_only: bool = False
) -> Optional[int]:
    """
    Create a single batch Kubernetes job to process multiple folders.

    Args:
        folders: List of relative folder paths to archive with their fingerprints [rel, fp]
        export_only: Whether to only export the job YAML without creating it

    Returns:
        Optional[int]: Number of folders processed or None if no action was taken
    """
    global ORIG, ZIP, DB, args

    if not folders:
        logger.info("No folders to process, skipping batch job creation")
        return

    # Generate timestamp for unique job name
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    try:
        # Load Jinja2 template
        template_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "job-batch-lidar-zip.template.yaml",
        )

        if not os.path.exists(template_path):
            logger.error(f"Template file not found at {template_path}")
            return

        with open(template_path, "r") as f:
            template_content = f.read()

        # Setup Jinja2 environment
        template = jinja2.Template(template_content)

        # Prepare template variables
        context = {
            "folders": folders,
            "timestamp": timestamp,
            "parallelism": args.parallelism,
            "orig_dir": ORIG,
            "zip_dir": ZIP,
            "db_path": DB,
            "db_dir": os.path.dirname(DB),
        }

        # Render the template
        job_yaml = template.render(**context)

        if export_only:
            print(job_yaml)
            logger.info(f"Printed batch job YAML for {len(folders)} folders")
            return

        # Create job from YAML
        import yaml
        from kubernetes import utils

        job_dict = yaml.safe_load(job_yaml)
        try:
            result = utils.create_from_dict(client.ApiClient(), job_dict, True)
            job_name = job_dict["metadata"]["name"]
            logger.info(f"Created batch job '{job_name}' for {len(folders)} folders")
            logger.debug(f"Job creation result: {result}")
            return len(folders)
        except Exception as api_ex:
            logger.error(f"Failed to create Kubernetes job via API: {api_ex}")
            raise
    except Exception as e:
        logger.error(f"Failed to create batch job: {e}")
        raise


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
        help="Root directory containing original LiDAR data",
    )
    parser.add_argument(
        "--zip-root",
        default="./zip_root",
        help="Root directory where compressed archives will be stored",
    )
    parser.add_argument(
        "--db-path",
        default="./state/archive.db",
        help="Path to the SQLite database file for tracking state",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set logging level (default: INFO)",
    )
    # parser.add_argument(
    #     "--execution-env",
    #     choices=["local", "docker", "kubernetes", "kube", "batch"],
    #     default="local",
    #     help="Execution environment for archive jobs (default: local)"
    # )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check for changes without modifying database or queueing jobs",
    )
    parser.add_argument(
        "--export-only",
        action="store_true",
        help="Print job YAMLs/commands instead of creating them",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=0,
        help="Stop after the specified number of archive jobs have been queued (0 for unlimited)",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=4,
        help="Number of parallel jobs to run in batch mode",
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
    execution_env = "batch"

    dry_run: bool = args.dry_run
    export_only: bool = args.export_only

    # Configuration validation moved here
    if not os.path.isdir(ORIG):
        logger.warning(
            f"Original root directory '{ORIG}' does not exist, creating it..."
        )
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
    logger.info(
        f"Options: execution_env='{execution_env}', log_level='{log_level}', "
        f"dry-run={dry_run}, export_only={export_only}"
    )

    # Load Kube config if using kubernetes modes
    if execution_env in ("kubernetes", "kube", "batch"):
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

    logger.info(f"Initializing database at path {DB}")
    db_manager = init_database(DB)
    logger.info(f"Initialized database at {DB}")

    # Process based on execution environment
    # Collect all changed folders first
    changed_folders = collect_changed_folders(db_manager, dry_run)

    # Limit folders if max_jobs is specified
    max_jobs = args.max_jobs
    length_changed_folders = len(changed_folders)
    if max_jobs > 0 and length_changed_folders > max_jobs:
        logger.info(
            f"Limiting to {max_jobs} out of {length_changed_folders} changed folders"
        )
        changed_folders = changed_folders[:max_jobs]

    # Create a single batch job for all folders
    if changed_folders:
        logger.info(f"Creating batch job for {length_changed_folders} changed folders")
        processed_count = queue_batch_zip_job(changed_folders, export_only)
        if processed_count:
            logger.info(f"Successfully created {processed_count} jobs")
    else:
        logger.info("No changes detected, no batch job needed")

    # Process metacloud files if requested
    metacloud_count = 0
    logger.info("Scanning for .metacloud files...")
    metacloud_changes = scan_for_metacloud_files(db_manager, dry_run)
    metacloud_count = len(metacloud_changes)

    if metacloud_changes:
        logger.info(f"Found {metacloud_count} .metacloud files to process")
        # Use max_jobs to limit metacloud files as well
        if max_jobs > 0 and metacloud_count > max_jobs:
            logger.info(
                f"Limiting to {max_jobs} out of {metacloud_count} metacloud files"
            )
            metacloud_changes = metacloud_changes[:max_jobs]
            metacloud_count = max_jobs

        potree_job_count = queue_potree_conversion_jobs(metacloud_changes, export_only)
        if potree_job_count:
            logger.info(
                f"Successfully created potree conversion job for {metacloud_count} files"
            )

    # Update completion message to include metacloud information
    logger.info(
        f"Scan completed: detected {length_changed_folders} folder changes"
        + (
            f" and {metacloud_count} metacloud changes"
            if metacloud_count > 0  # Changed from args.process_metacloud
            else ""
        )
    )


if __name__ == "__main__":
    main()
