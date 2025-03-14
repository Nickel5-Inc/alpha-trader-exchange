"""
Database analytics module for monitoring query performance and patterns.

This module provides analytics capabilities for:
1. Query performance tracking
2. Pattern analysis
3. Index usage monitoring
4. Table relationship analysis
"""

from typing import List, Dict, Optional
from datetime import datetime, timedelta

from .manager import DatabaseManager
from .query_tracker import QueryTracker, QueryStats

class DatabaseAnalytics:
    """Analytics class for monitoring database performance."""
    
    def __init__(self, slow_query_threshold: float = 1.0):
        """Initialize analytics manager.
        
        Args:
            slow_query_threshold: Threshold in seconds for slow queries
        """
        self._db = DatabaseManager()  # Get singleton instance
        self._tracker = QueryTracker(slow_query_threshold)
    
    def start_query(self, query: str) -> Dict:
        """Start tracking a query execution.
        
        Args:
            query: SQL query being executed
            
        Returns:
            Dict: Query context for tracking
        """
        return self._tracker.start_query(query)
    
    def end_query(
        self,
        context: Dict,
        rows_examined: int = 0,
        rows_returned: int = 0,
        index_used: bool = True
    ):
        """Record query completion.
        
        Args:
            context: Query context from start_query
            rows_examined: Number of rows examined
            rows_returned: Number of rows returned
            index_used: Whether an index was used
        """
        self._tracker.end_query(
            context,
            rows_examined=rows_examined,
            rows_returned=rows_returned,
            index_used=index_used
        )
    
    def start_transaction(self) -> int:
        """Start tracking a new transaction.
        
        Returns:
            int: Transaction ID
        """
        return self._tracker.start_transaction()
    
    def add_query_to_transaction(self, transaction_id: int, context: Dict):
        """Add a query to a transaction's tracking.
        
        Args:
            transaction_id: Transaction ID
            context: Query context
        """
        self._tracker.add_query_to_transaction(transaction_id, context)
    
    def end_transaction(self, transaction_id: int):
        """End tracking a transaction.
        
        Args:
            transaction_id: Transaction ID
        """
        self._tracker.end_transaction(transaction_id)
    
    def get_slow_queries(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict]:
        """Get slow query history.
        
        Args:
            start_time: Start of time range
            end_time: End of time range
            limit: Maximum number of queries to return
            
        Returns:
            List[Dict]: Slow query details sorted by duration
        """
        return self._tracker.get_slow_queries(start_time, end_time, limit)

    def get_query_patterns(self) -> List[QueryStats]:
        """Get common query patterns.
        
        Returns:
            List[QueryStats]: Query pattern statistics sorted by total time
        """
        return self._tracker.get_query_patterns()

    def get_index_effectiveness(self) -> Dict[str, Dict]:
        """Get index usage statistics.
        
        Returns:
            Dict mapping query patterns to index effectiveness metrics:
            - total_queries: Total number of executions
            - index_hit_ratio: Percentage of queries using indexes
            - avg_rows_examined: Average rows scanned per query
            - avg_rows_returned: Average rows returned per query
            - avg_time: Average execution time
        """
        return self._tracker.get_index_effectiveness()
    
    def get_table_relationships(self) -> Dict[str, set[str]]:
        """Get table access relationships.
        
        Returns:
            Dict mapping tables to sets of related tables that are
            commonly accessed together in queries.
        """
        return self._tracker.get_table_relationships()
    
    def analyze_query(self, query: str):
        """Analyze a query's table access patterns.
        
        Args:
            query: SQL query to analyze
        """
        self._tracker.analyze_table_access(query)
    
    def cleanup_old_data(self, max_age: timedelta = timedelta(days=7)):
        """Clean up old tracking data.
        
        Args:
            max_age: Maximum age of data to keep
        """
        # TODO: Implement index analysis
        return {} 