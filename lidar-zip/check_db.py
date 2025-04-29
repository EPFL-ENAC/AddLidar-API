#!/usr/bin/env python3
"""
Check the state of a specific folder in the SQLite database.
"""

import sqlite3
import sys
import os
import argparse
from datetime import datetime

def check_folder(db_path, folder_key=None):
    """
    Check the database entries for a specific folder or list all entries.
    
    Args:
        db_path: Path to the SQLite database
        folder_key: Optional folder key to check (e.g., "lvl1_01/lvl2_10")
    """
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    try:
        if folder_key:
            # Query for specific folder
            cursor = conn.execute(
                """SELECT folder_key, zip_path, fp, size_kb, file_count, 
                   last_seen, archived_at,
                   datetime(last_seen, 'unixepoch') as last_seen_time,
                   datetime(archived_at, 'unixepoch') as archived_time
                   FROM folder_state 
                   WHERE folder_key = ?""", 
                (folder_key,)
            )
            row = cursor.fetchone()
            
            if not row:
                print(f"No database entry found for folder: {folder_key}")
            else:
                print("Folder Database Entry:")
                print("=====================")
                print(f"Folder path:       {row['folder_key']}")
                print(f"Archive path:      {row['zip_path']}")
                print(f"Fingerprint:       {row['fp']}")
                print(f"Size (KB):         {row['size_kb']}")
                print(f"File count:        {row['file_count']}")
                print(f"Last seen:         {row['last_seen']} ({row['last_seen_time']})")
                print(f"Archived at:       {row['archived_at']} ({row['archived_time'] if row['archived_at'] else 'NULL'})")
                
                # Check if archive file exists
                if row['zip_path'] and os.path.exists(row['zip_path']):
                    size = os.path.getsize(row['zip_path'])
                    mod_time = datetime.fromtimestamp(os.path.getmtime(row['zip_path']))
                    print(f"Archive file:      EXISTS ({size/1024:.2f} KB, modified: {mod_time})")
                else:
                    print(f"Archive file:      MISSING")
        else:
            # Query for all folders
            cursor = conn.execute(
                """SELECT folder_key, zip_path, 
                   last_seen, archived_at,
                   datetime(last_seen, 'unixepoch') as last_seen_time,
                   datetime(archived_at, 'unixepoch') as archived_time
                   FROM folder_state
                   ORDER BY last_seen DESC
                   LIMIT 10"""
            )
            rows = cursor.fetchall()
            
            if not rows:
                print("No entries found in database")
            else:
                print(f"Most Recent 10 Database Entries:")
                print("==============================")
                for row in rows:
                    archive_status = "ARCHIVED" if row['archived_at'] else "PENDING"
                    print(f"{row['folder_key']:20} - {archive_status:8} - Last seen: {row['last_seen_time']}, Archived: {row['archived_time'] if row['archived_at'] else 'NULL'}")
                
                # Count statistics
                cursor = conn.execute(
                    """SELECT COUNT(*) as total, 
                       SUM(CASE WHEN archived_at IS NULL THEN 1 ELSE 0 END) as pending
                       FROM folder_state"""
                )
                stats = cursor.fetchone()
                print(f"\nTotal entries: {stats['total']}, Pending archives: {stats['pending']}")
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check folder state in the archive database")
    parser.add_argument("--db", default="./state/archive.db", help="Path to SQLite database")
    parser.add_argument("--folder", help="Specific folder key to check (e.g., 'lvl1_01/lvl2_10')")
    
    args = parser.parse_args()
    
    check_folder(args.db, args.folder)
