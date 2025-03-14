"""
Database utility functions.

This module provides utility functions for database operations such as:
- Timestamp parsing and formatting
- Data type conversions
- Common database operations
"""

from datetime import datetime, timezone

def parse_timestamp(timestamp_str: str) -> datetime:
    """Parse a timestamp string from the database into a timezone-aware datetime.
    
    Args:
        timestamp_str (str): Timestamp string from database
        
    Returns:
        datetime: Timezone-aware datetime object
    """
    dt = datetime.fromisoformat(timestamp_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

def format_timestamp(dt: datetime) -> str:
    """Format a datetime for storage in the database.
    
    Args:
        dt (datetime): Datetime to format
        
    Returns:
        str: ISO format string with timezone info
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat() 