"""
Query performance tracking and analysis.

This module provides tools to:
1. Track query execution times
2. Identify slow queries
3. Analyze query patterns
4. Monitor index usage
"""

import time
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
import logging
from collections import defaultdict, deque
import threading
from concurrent.futures import ThreadPoolExecutor
import sqlparse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class QueryStats:
    """Statistics for a specific query pattern."""
    pattern: str
    total_executions: int = 0
    total_time: float = 0.0
    min_time: float = float('inf')
    max_time: float = 0.0
    avg_time: float = 0.0
    last_execution: Optional[datetime] = None
    index_hits: int = 0
    index_misses: int = 0
    rows_examined: int = 0
    rows_returned: int = 0

class QueryTracker:
    """Tracks and analyzes database query performance."""
    
    def __init__(
        self,
        slow_query_threshold: float = 1.0,
        max_slow_queries: int = 1000,
        max_patterns: int = 1000
    ):
        """Initialize query tracker.
        
        Args:
            slow_query_threshold: Threshold in seconds for slow queries
            max_slow_queries: Maximum number of slow queries to keep
            max_patterns: Maximum number of query patterns to track
        """
        self.slow_query_threshold = slow_query_threshold
        self.max_slow_queries = max_slow_queries
        self.max_patterns = max_patterns
        
        # Use thread-safe data structures
        self._stats_lock = threading.Lock()
        self._stats: Dict[str, QueryStats] = {}
        
        # Use deque with maxlen for automatic pruning
        self._slow_queries = deque(maxlen=max_slow_queries)
        
        self._current_transactions: Dict[int, List[Dict]] = {}
        self._table_access_patterns: Dict[str, Set[str]] = defaultdict(set)
        self._last_cleanup = datetime.utcnow()
        
        # Thread pool for query parsing
        self._parser_pool = ThreadPoolExecutor(max_workers=4)
    
    def _normalize_query(self, query: str) -> str:
        """Normalize a query to group similar patterns.
        
        Uses sqlparse for proper SQL parsing and normalization.
        
        Args:
            query: SQL query to normalize
            
        Returns:
            str: Normalized query pattern
        """
        # Parse query
        parsed = sqlparse.parse(query)[0]
        
        # Normalize each token
        def normalize_token(token):
            if token.is_whitespace:
                return ' '
            if token.ttype in (sqlparse.tokens.Number, sqlparse.tokens.String):
                return '?'
            if token.is_group:
                return '(' + ''.join(normalize_token(t) for t in token.tokens) + ')'
            return str(token)
        
        # Join normalized tokens
        normalized = ''.join(normalize_token(token) for token in parsed.tokens)
        
        # Clean up whitespace
        return ' '.join(normalized.split())
    
    def start_query(self, query: str) -> Dict:
        """Start tracking a query execution.
        
        Args:
            query: SQL query being executed
            
        Returns:
            Dict: Query context for tracking
        """
        # Parse query in background thread
        future = self._parser_pool.submit(self._normalize_query, query)
        
        return {
            'query': query,
            'pattern_future': future,
            'start_time': time.time(),
            'start_datetime': datetime.utcnow()
        }
    
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
        duration = time.time() - context['start_time']
        pattern = context['pattern_future'].result()
        
        with self._stats_lock:
            # Update query stats
            if pattern not in self._stats:
                if len(self._stats) >= self.max_patterns:
                    # Remove oldest pattern
                    oldest = min(
                        self._stats.items(),
                        key=lambda x: x[1].last_execution or datetime.min
                    )
                    del self._stats[oldest[0]]
                self._stats[pattern] = QueryStats(pattern=pattern)
                
            stats = self._stats[pattern]
            stats.total_executions += 1
            stats.total_time += duration
            stats.min_time = min(stats.min_time, duration)
            stats.max_time = max(stats.max_time, duration)
            stats.avg_time = stats.total_time / stats.total_executions
            stats.last_execution = context['start_datetime']
            stats.rows_examined += rows_examined
            stats.rows_returned += rows_returned
            
            if index_used:
                stats.index_hits += 1
            else:
                stats.index_misses += 1
        
        # Track slow queries
        if duration >= self.slow_query_threshold:
            self._slow_queries.append({
                'query': context['query'],
                'pattern': pattern,
                'duration': duration,
                'timestamp': context['start_datetime'],
                'rows_examined': rows_examined,
                'rows_returned': rows_returned,
                'used_index': index_used
            })
    
    def start_transaction(self) -> int:
        """Start tracking a new transaction.
        
        Returns:
            int: Transaction ID
        """
        transaction_id = id(asyncio.current_task())
        self._current_transactions[transaction_id] = []
        return transaction_id
    
    def add_query_to_transaction(self, transaction_id: int, context: Dict):
        """Add a query to a transaction's tracking.
        
        Args:
            transaction_id: Transaction ID
            context: Query context
        """
        if transaction_id in self._current_transactions:
            self._current_transactions[transaction_id].append(context)
    
    def end_transaction(self, transaction_id: int):
        """End tracking a transaction.
        
        Args:
            transaction_id: Transaction ID
        """
        if transaction_id in self._current_transactions:
            del self._current_transactions[transaction_id]
    
    def analyze_table_access(self, query: str):
        """Analyze table access patterns in a query.
        
        Uses sqlparse for accurate table name extraction.
        
        Args:
            query: SQL query to analyze
        """
        parsed = sqlparse.parse(query)[0]
        tables = set()
        
        def extract_tables(token):
            if token.is_group:
                for t in token.tokens:
                    extract_tables(t)
            elif token.ttype in (sqlparse.tokens.Name, sqlparse.tokens.Name.Placeholder):
                prev_token = token.previous_token
                if prev_token and prev_token.value.upper() in ('FROM', 'JOIN', 'UPDATE', 'INTO'):
                    tables.add(token.value.strip('`"\''))
        
        for token in parsed.tokens:
            extract_tables(token)
        
        # Record table relationships
        tables = list(tables)
        for i in range(len(tables)):
            for j in range(i + 1, len(tables)):
                self._table_access_patterns[tables[i]].add(tables[j])
                self._table_access_patterns[tables[j]].add(tables[i])
    
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
            List[Dict]: Slow query details
        """
        queries = list(self._slow_queries)  # Convert deque to list
        
        if start_time:
            queries = [q for q in queries if q['timestamp'] >= start_time]
        if end_time:
            queries = [q for q in queries if q['timestamp'] <= end_time]
            
        # Sort by duration (slowest first)
        queries.sort(key=lambda x: x['duration'], reverse=True)
        return queries[:limit]
    
    def get_query_patterns(self) -> List[QueryStats]:
        """Get statistics for all query patterns.
        
        Returns:
            List[QueryStats]: Query pattern statistics
        """
        with self._stats_lock:
            # Sort by total time (most expensive first)
            return sorted(
                self._stats.values(),
                key=lambda x: x.total_time,
                reverse=True
            )
    
    def get_table_relationships(self) -> Dict[str, Set[str]]:
        """Get table access relationships.
        
        Returns:
            Dict[str, Set[str]]: Tables that are commonly accessed together
        """
        return dict(self._table_access_patterns)
    
    def get_index_effectiveness(self) -> Dict[str, Dict]:
        """Get index usage effectiveness by query pattern.
        
        Returns:
            Dict[str, Dict]: Index effectiveness metrics
        """
        with self._stats_lock:
            results = {}
            
            for pattern, stats in self._stats.items():
                total = stats.index_hits + stats.index_misses
                if total > 0:
                    hit_ratio = (stats.index_hits / total) * 100
                    results[pattern] = {
                        'total_queries': total,
                        'index_hit_ratio': hit_ratio,
                        'avg_rows_examined': stats.rows_examined / total,
                        'avg_rows_returned': stats.rows_returned / total,
                        'avg_time': stats.avg_time
                    }
            
            return results
    
    def cleanup_old_data(self, max_age: timedelta = timedelta(days=7)):
        """Clean up old tracking data.
        
        Args:
            max_age: Maximum age of data to keep
        """
        now = datetime.utcnow()
        
        # Only clean up once per hour
        if (now - self._last_cleanup).total_seconds() < 3600:
            return
            
        cutoff = now - max_age
        
        # Clean up query stats
        with self._stats_lock:
            for pattern in list(self._stats.keys()):
                stats = self._stats[pattern]
                if stats.last_execution and stats.last_execution < cutoff:
                    del self._stats[pattern]
        
        self._last_cleanup = now
    
    def __del__(self):
        """Clean up resources."""
        self._parser_pool.shutdown(wait=True) 