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


class FolderStateResponse(BaseModel):
    folder_key: str
    mission_key: str
    fp: str
    output_path: str
    size_kb: int
    file_count: int
    last_checked: int
    last_processed: Optional[int]
    processing_time: Optional[int]
    processing_status: Optional[str]
    error_message: Optional[str]


class PotreeMetacloudStateResponse(BaseModel):
    mission_key: str
    fp: Optional[str]
    output_path: Optional[str]
    last_checked: int
    last_processed: Optional[int]
    processing_time: Optional[int]
    processing_status: Optional[str]
    error_message: Optional[str]


# Database connection helper
def get_db_connection():
    try:
        # Get database path from settings, allow override via env var
        db_path = os.getenv("DATABASE_PATH", DATABASE_PATH)
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Return rows as dictionaries
        return conn
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {e} with path {db_path}")
        raise HTTPException(
            status_code=500, detail=f"Database connection failed: {str(e)}"
        )


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

        return [TableInfo(name=table["name"]) for table in tables]
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
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
            (table_name,),
        )
        if not cursor.fetchone():
            conn.close()
            raise HTTPException(
                status_code=404, detail=f"Table '{table_name}' not found"
            )

        # Get table schema
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()

        conn.close()

        return [ColumnInfo(name=col["name"], type=col["type"]) for col in columns]
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
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
            (table_name,),
        )
        if not cursor.fetchone():
            conn.close()
            raise HTTPException(
                status_code=404, detail=f"Table '{table_name}' not found"
            )

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
        count = cursor.fetchone()["count"]

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
    """Get folder state information with new schema"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Query with new schema
        query = """
        SELECT
          folder_key,
          mission_key,
          fp,
          output_path,
          size_kb,
          file_count,
          last_checked,
          last_processed,
          processing_time,
          processing_status,
          error_message,
          datetime(last_checked,'unixepoch') AS last_checked_time,
          datetime(last_processed,'unixepoch') AS last_processed_time
        FROM folder_state
        ORDER BY last_checked DESC
        LIMIT ? OFFSET ?
        """

        # Execute query
        cursor.execute(query, (limit, offset))
        rows = cursor.fetchall()

        # Get total count
        cursor.execute("SELECT COUNT(*) as count FROM folder_state")
        count = cursor.fetchone()["count"]

        conn.close()

        # Convert rows to list of dicts
        data = [dict(row) for row in rows]

        return QueryResult(data=data, count=count)
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Error querying folder state: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to query folder state: {str(e)}"
        )


@router.get("/folder_state/{subpath:path}", response_model=QueryResult)
async def get_folder_state_by_subpath(
    subpath: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> QueryResult:
    """Get folder state information for a specific subpath.
    Returns records where folder_key starts with the provided subpath.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Use the subpath as a prefix filter
        filter_value = f"{subpath}%"

        # Query with new schema filtered by subpath
        query = """
        SELECT
          folder_key,
          mission_key,
          fp,
          output_path,
          size_kb,
          file_count,
          last_checked,
          last_processed,
          processing_time,
          processing_status,
          error_message,
          datetime(last_checked,'unixepoch') AS last_checked_time,
          datetime(last_processed,'unixepoch') AS last_processed_time
        FROM folder_state
        WHERE folder_key LIKE ?
        ORDER BY last_checked DESC
        LIMIT ? OFFSET ?
        """

        # Execute query with subpath filter
        cursor.execute(query, (filter_value, limit, offset))
        rows = cursor.fetchall()

        # Get total count for the filtered subpath
        count_query = """
        SELECT COUNT(*) as count FROM folder_state WHERE folder_key LIKE ?
        """
        cursor.execute(count_query, (filter_value,))
        count = cursor.fetchone()["count"]

        conn.close()

        # Convert rows to list of dictionaries
        data = [dict(row) for row in rows]

        return QueryResult(data=data, count=count)
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Error querying folder state by subpath: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to query folder state by subpath: {str(e)}"
        )


@router.get("/folder_state/mission/{mission_key}", response_model=QueryResult)
async def get_folder_state_by_mission(
    mission_key: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> QueryResult:
    """Get folder state information for a specific mission key."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Query filtered by mission_key
        query = """
        SELECT
          folder_key,
          mission_key,
          fp,
          output_path,
          size_kb,
          file_count,
          last_checked,
          last_processed,
          processing_time,
          processing_status,
          error_message,
          datetime(last_checked,'unixepoch') AS last_checked_time,
          datetime(last_processed,'unixepoch') AS last_processed_time
        FROM folder_state
        WHERE mission_key = ?
        ORDER BY last_checked DESC
        LIMIT ? OFFSET ?
        """

        # Execute query with mission_key filter
        cursor.execute(query, (mission_key, limit, offset))
        rows = cursor.fetchall()

        # Get total count for the mission
        count_query = """
        SELECT COUNT(*) as count FROM folder_state WHERE mission_key = ?
        """
        cursor.execute(count_query, (mission_key,))
        count = cursor.fetchone()["count"]

        conn.close()

        # Convert rows to list of dictionaries
        data = [dict(row) for row in rows]

        return QueryResult(data=data, count=count)
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Error querying folder state by mission: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to query folder state by mission: {str(e)}"
        )


@router.get("/potree_metacloud_state", response_model=QueryResult)
async def get_potree_metacloud_state(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Get potree metacloud state information"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Query potree_metacloud_state table
        query = """
        SELECT
          mission_key,
          fp,
          output_path,
          last_checked,
          last_processed,
          processing_time,
          processing_status,
          error_message,
          datetime(last_checked,'unixepoch') AS last_checked_time,
          datetime(last_processed,'unixepoch') AS last_processed_time
        FROM potree_metacloud_state
        ORDER BY last_checked DESC
        LIMIT ? OFFSET ?
        """

        # Execute query
        cursor.execute(query, (limit, offset))
        rows = cursor.fetchall()

        # Get total count
        cursor.execute("SELECT COUNT(*) as count FROM potree_metacloud_state")
        count = cursor.fetchone()["count"]

        conn.close()

        # Convert rows to list of dicts
        data = [dict(row) for row in rows]

        return QueryResult(data=data, count=count)
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Error querying potree metacloud state: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to query potree metacloud state: {str(e)}"
        )


@router.get("/potree_metacloud_state/{mission_key}", response_model=Dict[str, Any])
async def get_potree_metacloud_state_by_mission(mission_key: str):
    """Get potree metacloud state for a specific mission"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Query for specific mission
        query = """
        SELECT
          mission_key,
          fp,
          output_path,
          last_checked,
          last_processed,
          processing_time,
          processing_status,
          error_message,
          datetime(last_checked,'unixepoch') AS last_checked_time,
          datetime(last_processed,'unixepoch') AS last_processed_time
        FROM potree_metacloud_state
        WHERE mission_key = ?
        """

        cursor.execute(query, (mission_key,))
        row = cursor.fetchone()

        conn.close()

        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Potree metacloud state not found for mission: {mission_key}",
            )

        return dict(row)
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Error querying potree metacloud state: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to query potree metacloud state: {str(e)}"
        )


@router.get("/processing_status", response_model=QueryResult)
async def get_processing_status():
    """Get processing status overview for both folder_state and potree_metacloud_state"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Combined query to get processing status overview
        query = """
        SELECT 
          'folder_state' as table_name,
          processing_status,
          COUNT(*) as count
        FROM folder_state 
        GROUP BY processing_status
        
        UNION ALL
        
        SELECT 
          'potree_metacloud_state' as table_name,
          processing_status,
          COUNT(*) as count
        FROM potree_metacloud_state 
        GROUP BY processing_status
        
        ORDER BY table_name, processing_status
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        conn.close()

        # Convert rows to list of dicts
        data = [dict(row) for row in rows]
        count = len(data)

        return QueryResult(data=data, count=count)
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Error querying processing status: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to query processing status: {str(e)}"
        )


@router.get("/settings", response_model=Dict[str, Any])
async def get_settings():
    """Get current settings"""
    try:
        # Convert settings to dictionary
        settings_dict = settings.model_dump()
        return {"settings": settings_dict}
    except Exception as e:
        logger.error(f"Error fetching settings: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch settings: {str(e)}"
        )
