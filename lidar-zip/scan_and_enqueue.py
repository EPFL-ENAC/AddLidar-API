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
                        "SELECT fp FROM folder_state WHERE folder_key=?", (rel,)
                    )
                    row = cursor.fetchone()

                if not row or row[0] != fp:
                    logger.info(f"Change detected in {rel}")
                    changed_folders.append([rel, fp])

                    if not dry_run:
                        with db_manager.get_connection() as conn:
                            conn.execute(
                                """INSERT INTO folder_state
                                (folder_key, fp, size_kb, file_count, last_seen, archived_at, zip_path)
                                VALUES (?, ?, ?, ?, ?, NULL, ?)
                                ON CONFLICT(folder_key) DO UPDATE SET
                                size_kb = excluded.size_kb,
                                file_count = excluded.file_count,
                                last_seen = excluded.last_seen,
                                archived_at = NULL,
                                zip_path = excluded.zip_path""",
                                (
                                    rel,
                                    fp,
                                    size,
                                    count,
                                    int(time.time()),
                                    os.path.join(ZIP, f"{rel}.tar.gz"),
                                ),
                            )
                            conn.commit()
            except Exception as e:
                logger.error(f"Error processing directory {rel}: {e}")

    return changed_folders


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

    logger.info(f"Scan completed: detected {length_changed_folders} changes")


if __name__ == "__main__":
    main()
