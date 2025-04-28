from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import sqlite3
import logging
import os
from pathlib import Path

# Import settings
from src.config.settings import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
DATABASE_PATH = settings.DATABASE_PATH

# Create router
router = APIRouter(
    prefix="/sqlite",
    tags=["sqlite"],
    responses={404: {"description": "Not found"}},
)

# Pydantic models for responses
class TableInfo(BaseModel):
    name: str
    
class ColumnInfo(BaseModel):
    name: str
    type: str
    
class QueryResult(BaseModel):
    data: List[Dict[str, Any]]
    count: int

# Database connection helper
def get_db_connection():
    try:
        # Get database path from settings, allow override via env var
        db_path = os.getenv("DATABASE_PATH", DATABASE_PATH)
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Return rows as dictionaries
        return conn
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@router.get("/tables", response_model=List[TableInfo])
async def get_tables():
    """Get list of all tables in the SQLite database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Query to get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        conn.close()
        
        return [TableInfo(name=table['name']) for table in tables]
    except Exception as e:
        logger.error(f"Error fetching tables: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch tables: {str(e)}")

@router.get("/schema/{table_name}", response_model=List[ColumnInfo])
async def get_table_schema(table_name: str):
    """Get schema information for a specific table"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        if not cursor.fetchone():
            conn.close()
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        
        # Get table schema
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        conn.close()
        
        return [ColumnInfo(name=col['name'], type=col['type']) for col in columns]
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Error fetching schema: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch schema: {str(e)}")

@router.get("/query/{table_name}", response_model=QueryResult)
async def query_table(
    table_name: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    columns: Optional[str] = None,
    where: Optional[str] = None,
):
    """Query data from a table with optional filters"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        if not cursor.fetchone():
            conn.close()
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        
        # Build query
        selected_columns = "*"
        if columns:
            selected_columns = columns
        
        query = f"SELECT {selected_columns} FROM {table_name}"
        count_query = f"SELECT COUNT(*) as count FROM {table_name}"
        
        if where:
            query += f" WHERE {where}"
            count_query += f" WHERE {where}"
            
        query += f" LIMIT {limit} OFFSET {offset}"
        
        # Execute query
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Get total count
        cursor.execute(count_query)
        count = cursor.fetchone()['count']
        
        conn.close()
        
        # Convert rows to list of dicts
        data = [dict(row) for row in rows]
        
        return QueryResult(data=data, count=count)
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Error querying table: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to query table: {str(e)}")

@router.get("/folder_state", response_model=QueryResult)
async def get_folder_state(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Get folder state information matching the export.sh format"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Query matching the export.sh format
        query = """
        SELECT
          folder_key                     AS folder_path,
          size_kb                        AS folder_size_kb,
          file_count                     AS folder_file_count,
          zip_path                       AS archive_path,
          archived_at                    AS archive_mod_time_epoch,
          datetime(archived_at,'unixepoch') AS archive_mod_time,
          last_seen                      AS folder_mod_time_epoch,
          datetime(last_seen,'unixepoch')   AS folder_mod_time
        FROM folder_state
        ORDER BY last_seen DESC
        LIMIT ? OFFSET ?
        """
        
        # Execute query
        cursor.execute(query, (limit, offset))
        rows = cursor.fetchall()
        
        # Get total count
        cursor.execute("SELECT COUNT(*) as count FROM folder_state")
        count = cursor.fetchone()['count']
        
        conn.close()
        
        # Convert rows to list of dicts
        data = [dict(row) for row in rows]
        
        return QueryResult(data=data, count=count)
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Error querying folder state: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to query folder state: {str(e)}")