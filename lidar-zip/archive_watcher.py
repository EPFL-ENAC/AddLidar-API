#!/usr/bin/env python3
# /// script
# dependencies = [
#   "watchdog",
#   "pydantic",
# ]
# ///
"""
LiDAR Archive Completion Monitor

This script monitors the creation of tar.gz archives and updates the database
when an archive is successfully created.
"""

import os
import time
import sqlite3
import logging
from typing import Dict, Set
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent

# Configure logging
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("archive-watcher")

# Constants from environment variables with defaults
ZIP = os.environ.get("ZIP_ROOT", "./zip_root")
DB = os.environ.get("DB_PATH", "./state/archive.db")
UPDATE_INTERVAL = int(os.environ.get("UPDATE_INTERVAL", "60"))  # seconds between database updates
MIN_FILE_AGE = int(os.environ.get("MIN_FILE_AGE", "10"))  # seconds since last modification to consider file complete


class ArchiveEventHandler(FileSystemEventHandler):
    """Handler for file system events related to archives."""
    
    def __init__(self, db_path: str, zip_root: str) -> None:
        """
        Initialize the archive event handler.
        
        Args:
            db_path: Path to the SQLite database
            zip_root: Root directory for archives
        """
        super().__init__()
        self.db_path = db_path
        self.zip_root = zip_root
        self.completed_archives: Set[str] = set()
        self.pending_updates: Dict[str, int] = {}
        
        # Initialize the observer with a periodic database update thread
        self.observer = Observer()
        self.observer.schedule(self, zip_root, recursive=True)
        
        # Thread for periodically processing archive updates
        self.update_thread = threading.Thread(target=self._periodic_update, daemon=True)
        self.running = True
    
    def on_created(self, event) -> None:
        """
        Handle file creation events.
        
        Args:
            event: The file system event
        """
        if not event.is_directory and event.src_path.endswith('.tar.gz'):
            logger.debug(f"Archive created: {event.src_path}")
            self._queue_archive_update(event.src_path)
    
    def on_modified(self, event) -> None:
        """
        Handle file modification events.
        
        Args:
            event: The file system event
        """
        if not event.is_directory and event.src_path.endswith('.tar.gz'):
            logger.debug(f"Archive modified: {event.src_path}")
            self._queue_archive_update(event.src_path)
    
    def _queue_archive_update(self, archive_path: str) -> None:
        """
        Queue an archive for database update.
        
        Args:
            archive_path: Path to the archive file
        """
        # Record the current time for this archive
        self.pending_updates[archive_path] = int(time.time())
    
    def _periodic_update(self) -> None:
        """Periodically process pending archive updates."""
        while self.running:
            try:
                now = int(time.time())
                to_update: Set[str] = set()
                
                # Find archives that have been stable for MIN_FILE_AGE seconds
                for path, timestamp in list(self.pending_updates.items()):
                    if now - timestamp >= MIN_FILE_AGE:
                        # Check if file exists and hasn't been modified recently
                        try:
                            file_mtime = os.path.getmtime(path)
                            if now - file_mtime >= MIN_FILE_AGE:
                                to_update.add(path)
                                del self.pending_updates[path]
                        except (FileNotFoundError, PermissionError) as e:
                            logger.warning(f"Error checking file {path}: {e}")
                            del self.pending_updates[path]
                
                # Process all stable archives
                if to_update:
                    self._update_database(to_update)
                
                # Sleep until next check
                time.sleep(min(10, UPDATE_INTERVAL))  # Check at least every 10 seconds
            except Exception as e:
                logger.error(f"Error in periodic update: {e}")
                time.sleep(10)  # Sleep and retry on error
    
    def _update_database(self, archive_paths: Set[str]) -> None:
        """
        Update the database for completed archives.
        
        Args:
            archive_paths: Set of paths to completed archives
        """
        if not archive_paths:
            return
            
        logger.info(f"Updating {len(archive_paths)} completed archives in database")
        
        # Connect to the database
        db = sqlite3.connect(self.db_path)
        try:
            now = int(time.time())
            count = 0
            
            for path in archive_paths:
                try:
                    # Strip prefix and suffix to get the folder key
                    rel_path = os.path.relpath(path, self.zip_root)
                    if rel_path.endswith('.tar.gz'):
                        folder_key = rel_path[:-7]  # Remove .tar.gz suffix
                        
                        # Update the database
                        cursor = db.execute(
                            "UPDATE folder_state SET archived_at = ? WHERE folder_key = ? AND archived_at IS NULL",
                            (now, folder_key)
                        )
                        
                        if cursor.rowcount > 0:
                            count += 1
                            logger.info(f"Updated archive completion for folder: {folder_key}")
                except Exception as e:
                    logger.error(f"Failed to update database for {path}: {e}")
            
            # Commit all changes
            db.commit()
            logger.info(f"Successfully updated {count} archives in the database")
        except Exception as e:
            logger.error(f"Database update error: {e}")
            db.rollback()
        finally:
            db.close()
    
    def start(self) -> None:
        """Start the archive watcher."""
        self.observer.start()
        self.update_thread.start()
        logger.info(f"Archive watcher started, monitoring {self.zip_root}")
    
    def stop(self) -> None:
        """Stop the archive watcher."""
        self.running = False
        self.observer.stop()
        self.observer.join()
        logger.info("Archive watcher stopped")


def main() -> None:
    """Main function to run the archive watcher."""
    # Ensure directories exist
    os.makedirs(ZIP, exist_ok=True)
    os.makedirs(os.path.dirname(DB), exist_ok=True)
    
    # Create and start the event handler
    handler = ArchiveEventHandler(DB, ZIP)
    try:
        handler.start()
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down")
    finally:
        handler.stop()


if __name__ == "__main__":
    main()
