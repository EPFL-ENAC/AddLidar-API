#!/usr/bin/env python3
# filepath: /Users/pierreguilbert/Works/git/github/EPFL-ENAC/AddLidar-API/lidar-zip/rebuild_db.py

import os
import sqlite3
import hashlib
import logging
import time
import subprocess
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import argparse
from datetime import datetime
from dataclasses import dataclass
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger("db-rebuilder")

# Default paths (will be overridable via command-line arguments)
ORIG = os.environ.get("ORIGINAL_ROOT", "./original_root")
ZIP = os.environ.get("ZIP_ROOT", "./zip_root")
DB_PATH = os.environ.get("DB_PATH", "./state/archive.db")


@dataclass
class ArchiveInfo:
    """Class for holding archive information for database reconstruction."""
    folder_key: str
    zip_path: str
    archived_at: int
    size_kb: int = 0  # Will be filled from original if available
    file_count: int = 0  # Will be filled from original if available
    fp: Optional[str] = None  # Will be calculated if original exists


def fingerprint(path: str) -> str:
    """
    Generate a unique fingerprint for a directory based on file attributes.
    
    Args:
        path: Directory path to fingerprint
        
    Returns:
        SHA-256 hash representing the directory content state
    """
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
            file_data = f"{rel_path}|{size_bytes}|{mod_time}\n".encode('utf-8')
            hasher.update(file_data)
        
        return hasher.hexdigest()
    except Exception as e:
        logger.error(f"Failed to generate fingerprint for {path}: {e}")
        # Return a placeholder hash if we can't calculate properly
        return hashlib.sha256(path.encode()).hexdigest()


def get_directory_stats(path: str) -> Tuple[str, int, int]:
    """
    Get directory statistics: fingerprint, size in KB, and file count.
    
    Args:
        path: Path to directory
        
    Returns:
        Tuple containing (fingerprint, size_kb, file_count)
    """
    try:
        fp = fingerprint(path)
        
        try:
            # Get directory size in KB
            size = int(subprocess.check_output(["du", "-sk", path]).split()[0])
        except (subprocess.SubprocessError, ValueError):
            # Fallback if du fails
            size = sum(os.path.getsize(os.path.join(dirpath, filename)) 
                     for dirpath, _, filenames in os.walk(path)
                     for filename in filenames) // 1024
        
        try:
            # Get file count
            count = int(subprocess.check_output(["bash", "-c", f"find '{path}' -type f | wc -l"]).strip())
        except (subprocess.SubprocessError, ValueError):
            # Fallback if find+wc fails
            count = sum(len(filenames) for _, _, filenames in os.walk(path))
        
        return fp, size, count
    except Exception as e:
        logger.error(f"Failed to get stats for {path}: {e}")
        # Return placeholder values
        return hashlib.sha256(path.encode()).hexdigest(), 0, 0


def init_database(db_path: str) -> None:
    """
    Initialize a new SQLite database with the folder_state schema.
    
    Args:
        db_path: Path to the SQLite database file
    """
    # Ensure directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Create database and initialize schema
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout = 10000")  # 10 seconds
        
        # Create the folder_state table
        conn.execute("""
        CREATE TABLE IF NOT EXISTS folder_state (
            folder_key TEXT PRIMARY KEY, 
            fp TEXT,
            size_kb INT, 
            file_count INT,
            last_seen INT, 
            archived_at INT, 
            zip_path TEXT
        )
        """)
        
        # Create index
        conn.execute("CREATE INDEX IF NOT EXISTS idx_folder_key ON folder_state (folder_key)")
        
        logger.info(f"Initialized database schema at {db_path}")
    finally:
        conn.close()


def scan_archives(zip_root: str, original_root: str) -> List[ArchiveInfo]:
    """
    Scan the zip root directory for tar.gz archives and extract their information.
    
    Args:
        zip_root: Path to the root of zip archives
        original_root: Path to the original files
        
    Returns:
        List of ArchiveInfo objects
    """
    archives = []
    zip_root_path = Path(zip_root)
    original_root_path = Path(original_root)
    
    # Regular expression to match tar.gz archives
    archive_pattern = re.compile(r'(.+)\.tar\.gz$')
    
    for archive_path in Path(zip_root).glob('**/*.tar.gz'):
        # Get relative path from the zip root
        rel_path = archive_path.relative_to(zip_root_path)
        
        # Extract folder_key (strip .tar.gz extension)
        folder_key = str(rel_path)
        match = archive_pattern.match(folder_key)
        if match:
            folder_key = match.group(1)
        else:
            continue
        
        # Get archive stats
        stat = archive_path.stat()
        archived_at = int(stat.st_mtime)
        
        # Ensure archive path has the correct prefix
        formatted_zip_path = str(archive_path)
        if not formatted_zip_path.startswith("./"):
            # If zip_root itself starts with "./", don't add it again
            if not zip_root.startswith("./"):
                formatted_zip_path = f"./{formatted_zip_path}"
        
        # Create archive info
        archive_info = ArchiveInfo(
            folder_key=folder_key,
            zip_path=formatted_zip_path,
            archived_at=archived_at
        )
        
        # Check if original directory exists
        original_dir = original_root_path / folder_key
        if original_dir.is_dir():
            # Get original directory stats
            try:
                fp, size_kb, file_count = get_directory_stats(str(original_dir))
                archive_info.fp = fp
                archive_info.size_kb = size_kb
                archive_info.file_count = file_count
                logger.info(f"Found matching original directory for {folder_key}")
            except Exception as e:
                logger.warning(f"Error getting stats for original directory {folder_key}: {e}")
        else:
            logger.warning(f"Original directory not found for {folder_key}")
            # Generate placeholder fingerprint based on archive content
            archive_info.fp = hashlib.sha256(f"{folder_key}|{archived_at}".encode()).hexdigest()
        
        archives.append(archive_info)
        logger.debug(f"Processed archive: {archive_info.folder_key}")
    
    return archives


def populate_database(db_path: str, archives: List[ArchiveInfo]) -> None:
    """
    Populate the SQLite database using data from archive scan.
    
    Args:
        db_path: Path to the SQLite database file
        archives: List of ArchiveInfo objects
    """
    # Connect to the database
    conn = sqlite3.connect(db_path)
    try:
        # Insert each archive record
        for archive in archives:
            # Current timestamp for last_seen if we don't have better information
            last_seen = archive.archived_at
            
            conn.execute("""
            INSERT INTO folder_state
            (folder_key, fp, size_kb, file_count, last_seen, archived_at, zip_path)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                archive.folder_key,
                archive.fp,
                archive.size_kb,
                archive.file_count,
                last_seen,
                archive.archived_at,
                archive.zip_path
            ))
        
        # Commit all changes
        conn.commit()
        logger.info(f"Successfully populated database with {len(archives)} folder records")
    finally:
        conn.close()


def verify_database(db_path: str) -> None:
    """
    Verify the database was properly populated.
    
    Args:
        db_path: Path to the SQLite database file
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute("SELECT COUNT(*) FROM folder_state")
        count = cursor.fetchone()[0]
        logger.info(f"Database verification: {count} records in folder_state table")
        
        # Show some sample records
        cursor = conn.execute("""
        SELECT folder_key, substr(fp, 1, 8) || '...', size_kb, file_count, 
               datetime(last_seen, 'unixepoch'), datetime(archived_at, 'unixepoch'), zip_path 
        FROM folder_state LIMIT 5
        """)
        
        logger.info("Sample records:")
        rows = cursor.fetchall()
        for row in rows:
            folder_key, fp_prefix, size_kb, file_count, last_seen, archived_at, zip_path = row
            logger.info(f"  {folder_key}: fp={fp_prefix}, size={size_kb}KB, files={file_count}, archived={archived_at}, zip={os.path.basename(zip_path)}")
    finally:
        conn.close()


def main() -> None:
    """
    Main function to rebuild the database.
    """
    parser = argparse.ArgumentParser(description="Rebuild archive database from existing directory structure")
    parser.add_argument("--orig", default=ORIG, help="Path to original data directory")
    parser.add_argument("--zip", default=ZIP, help="Path to zip archives directory")
    parser.add_argument("--db", default=DB_PATH, help="Path to SQLite database file")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--force", action="store_true", help="Force database recreation if it already exists")
    args = parser.parse_args()
    
    # Set debug logging if requested
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    
    logger.info(f"Starting database rebuild from directory structure")
    logger.info(f"  Original directory: {args.orig}")
    logger.info(f"  Zip directory: {args.zip}")
    logger.info(f"  Database path: {args.db}")
    
    # Check if directories exist
    if not os.path.isdir(args.orig):
        logger.error(f"Original directory not found: {args.orig}")
        return
    
    if not os.path.isdir(args.zip):
        logger.error(f"Zip directory not found: {args.zip}")
        return
    
    # Check if database already exists
    if os.path.exists(args.db) and not args.force:
        logger.error(f"Database already exists: {args.db}")
        logger.error("Use --force to overwrite existing database")
        return
    elif os.path.exists(args.db):
        backup_path = f"{args.db}.bak.{int(time.time())}"
        logger.info(f"Creating backup at: {backup_path}")
        os.rename(args.db, backup_path)
    
    # Initialize the database
    init_database(args.db)
    
    # Scan archive directories
    logger.info("Scanning archive directories...")
    archives = scan_archives(args.zip, args.orig)
    logger.info(f"Found {len(archives)} archives")
    
    # Populate the database
    logger.info("Populating database...")
    populate_database(args.db, archives)
    
    # Verify the database
    verify_database(args.db)
    
    logger.info(f"Database rebuild complete: {args.db}")
    logger.info("You can now use the database with the scan_and_enqueue.py script")


if __name__ == "__main__":
    main()